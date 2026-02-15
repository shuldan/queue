package queue

import (
	"context"
	"fmt"
	"reflect"
	"runtime/debug"
	"sync"
	"time"
)

const dlqTopicPrefix = "dlq:"

type Queue[T any] struct {
	topic      string
	broker     Broker
	opts       *options
	middleware []Middleware[T]
	ctx        context.Context
	cancel     context.CancelFunc
	mu         sync.RWMutex
	wg         sync.WaitGroup
	closed     bool
}

func New[T any](broker Broker, opts ...Option) (*Queue[T], error) {
	topic, err := resolveTopicFromType[T]()
	if err != nil {
		return nil, err
	}

	o := defaultOptions()
	for _, opt := range opts {
		opt(o)
	}

	if err = validateOptions(o); err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &Queue[T]{
		topic:  topic,
		broker: broker,
		opts:   o,
		ctx:    ctx,
		cancel: cancel,
	}, nil
}

func resolveTopicFromType[T any]() (string, error) {
	var t T
	typ := reflect.TypeOf(t)

	if typ == nil {
		return "", ErrInvalidJobType
	}

	if typ.Kind() != reflect.Ptr || typ.Elem().Kind() != reflect.Struct {
		return "", ErrInvalidJobType
	}

	return typ.Elem().String(), nil
}

func (q *Queue[T]) Produce(
	ctx context.Context, job T, opts ...ProduceOption,
) error {
	if err := q.checkClosed(); err != nil {
		return err
	}

	po := &produceOptions{}
	for _, opt := range opts {
		opt(po)
	}

	return q.produceEnvelope(ctx, job, po.headers)
}

func (q *Queue[T]) produceEnvelope(
	ctx context.Context, job T, headers map[string]string,
) error {
	data, err := q.opts.serializer.Marshal(job)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrMarshal, err)
	}

	topic := q.getTopicName()
	env := newEnvelope(topic, data, headers)

	envData, err := marshalEnvelope(env)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrMarshal, err)
	}

	return q.broker.Produce(ctx, topic, envData)
}

func (q *Queue[T]) Consume(
	ctx context.Context,
	handler func(context.Context, T) error,
) error {
	if handler == nil {
		return ErrNilHandler
	}

	if err := q.addConsumer(); err != nil {
		return err
	}
	defer q.wg.Done()

	return q.runConsumer(ctx, handler)
}

func (q *Queue[T]) addConsumer() error {
	q.mu.RLock()
	defer q.mu.RUnlock()

	if q.closed {
		return ErrQueueClosed
	}

	q.wg.Add(1)

	return nil
}

func (q *Queue[T]) runConsumer(
	ctx context.Context,
	handler func(context.Context, T) error,
) error {
	mergedCtx, mergedCancel := q.mergeContext(ctx)
	defer mergedCancel()

	q.mu.RLock()
	mws := make([]Middleware[T], len(q.middleware))
	copy(mws, q.middleware)
	q.mu.RUnlock()

	finalHandler := chainMiddleware(handler, mws)
	jobs := make(chan []byte, q.opts.bufferSize)

	workerWg := q.startWorkers(mergedCtx, jobs, finalHandler)
	defer func() {
		close(jobs)
		workerWg.Wait()
	}()

	topic := q.getTopicName()

	return q.broker.Consume(
		mergedCtx, topic, q.enqueueJob(mergedCtx, jobs),
	)
}

func (q *Queue[T]) Close() error {
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return nil
	}
	q.closed = true
	q.mu.Unlock()

	q.cancel()
	q.wg.Wait()

	return nil
}

func (q *Queue[T]) Use(mw ...Middleware[T]) {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.middleware = append(q.middleware, mw...)
}

func (q *Queue[T]) Ping(ctx context.Context) error {
	return q.broker.Ping(ctx)
}

func (q *Queue[T]) Topic() string { return q.getTopicName() }

func (q *Queue[T]) DLQTopic() string { return q.getDLQTopic() }

func (q *Queue[T]) mergeContext(
	parent context.Context,
) (context.Context, context.CancelFunc) {
	merged, mergedCancel := context.WithCancel(parent)

	q.wg.Add(1)

	go func() {
		defer q.wg.Done()

		select {
		case <-q.ctx.Done():
			mergedCancel()
		case <-merged.Done():
		}
	}()

	return merged, mergedCancel
}

func (q *Queue[T]) enqueueJob(
	ctx context.Context, jobs chan<- []byte,
) func([]byte) error {
	return func(data []byte) error {
		select {
		case jobs <- data:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (q *Queue[T]) startWorkers(
	ctx context.Context,
	jobs <-chan []byte,
	handler func(context.Context, T) error,
) *sync.WaitGroup {
	var wg sync.WaitGroup

	for range q.opts.workerCount {
		wg.Add(1)

		go func() {
			defer wg.Done()
			q.worker(ctx, jobs, handler)
		}()
	}

	return &wg
}

func (q *Queue[T]) worker(
	ctx context.Context,
	jobs <-chan []byte,
	handler func(context.Context, T) error,
) {
	for data := range jobs {
		q.processJob(ctx, data, handler)
	}
}

func (q *Queue[T]) processJob(
	ctx context.Context,
	data []byte,
	handler func(context.Context, T) error,
) {
	env, job, err := q.decodeJob(data)
	if err != nil {
		q.opts.errorHandler.Handle(ErrorContext{
			Topic: q.getTopicName(),
			Err:   fmt.Errorf("%w: %w", ErrUnmarshal, err),
		})

		return
	}

	q.executeWithRetries(ctx, env, job, handler)
}

func (q *Queue[T]) decodeJob(data []byte) (*Envelope, T, error) {
	var zero T

	env, err := unmarshalEnvelope(data)
	if err != nil {
		return nil, zero, err
	}

	var job T
	if err = q.opts.serializer.Unmarshal(env.Data, &job); err != nil {
		return nil, zero, err
	}

	return env, job, nil
}

func (q *Queue[T]) executeWithRetries(
	ctx context.Context,
	env *Envelope,
	job T,
	handler func(context.Context, T) error,
) {
	for attempt := range q.opts.maxRetries + 1 {
		jobCtx := WithMeta(ctx, q.buildMeta(env, attempt))

		if err := q.safeExecute(jobCtx, env, job, handler); err == nil {
			return
		} else {
			q.reportError(env, attempt, err)
		}

		if attempt < q.opts.maxRetries && !q.waitBackoff(ctx, attempt) {
			return
		}
	}

	if q.opts.dlqEnabled {
		q.sendToDLQ(env)
	}
}

func (q *Queue[T]) safeExecute(
	ctx context.Context,
	env *Envelope,
	job T,
	handler func(context.Context, T) error,
) (retErr error) {
	defer func() {
		if r := recover(); r != nil {
			q.opts.panicHandler.Handle(PanicContext{
				Topic:      env.Topic,
				MessageID:  env.ID,
				PanicValue: r,
				Stack:      debug.Stack(),
			})
			retErr = fmt.Errorf("panic recovered: %v", r)
		}
	}()

	return handler(ctx, job)
}

func (q *Queue[T]) buildMeta(env *Envelope, attempt int) *MessageMeta {
	return &MessageMeta{
		ID:        env.ID,
		Topic:     env.Topic,
		Attempt:   env.Attempt + attempt,
		Headers:   env.Headers,
		CreatedAt: env.CreatedAt,
	}
}

func (q *Queue[T]) reportError(env *Envelope, attempt int, err error) {
	q.opts.errorHandler.Handle(ErrorContext{
		Topic:     env.Topic,
		MessageID: env.ID,
		Attempt:   env.Attempt + attempt,
		Err:       err,
	})
}

func (q *Queue[T]) waitBackoff(ctx context.Context, attempt int) bool {
	delay := q.opts.backoff.Delay(attempt)
	if delay <= 0 {
		return true
	}

	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}

const dlqTimeout = 5 * time.Second

func (q *Queue[T]) sendToDLQ(env *Envelope) {
	data, err := marshalEnvelope(env)
	if err != nil {
		q.opts.errorHandler.Handle(ErrorContext{
			Topic:     env.Topic,
			MessageID: env.ID,
			Err:       fmt.Errorf("%w: %w", ErrMarshal, err),
		})

		return
	}

	dlqCtx, cancel := context.WithTimeout(context.Background(), dlqTimeout)
	defer cancel()

	if err = q.broker.Produce(dlqCtx, q.getDLQTopic(), data); err != nil {
		q.opts.errorHandler.Handle(ErrorContext{
			Topic:     env.Topic,
			MessageID: env.ID,
			Err:       fmt.Errorf("%w: %w", ErrSendToDLQ, err),
		})
	}
}

func (q *Queue[T]) checkClosed() error {
	q.mu.RLock()
	defer q.mu.RUnlock()

	if q.closed {
		return ErrQueueClosed
	}

	return nil
}

func (q *Queue[T]) getTopicName() string {
	topic := q.topic
	if q.opts.topic != "" {
		topic = q.opts.topic
	}

	return q.opts.prefix + topic
}

func (q *Queue[T]) getDLQTopic() string {
	topic := q.topic
	if q.opts.topic != "" {
		topic = q.opts.topic
	}

	return q.opts.prefix + dlqTopicPrefix + topic
}
