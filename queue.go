package queue

import (
	"context"
	"encoding/json"
	"reflect"
	"runtime"
	"runtime/debug"
	"sync"
	"time"
)

type Broker interface {
	Produce(ctx context.Context, topic string, data []byte) error
	Consume(ctx context.Context, topic string, handler func([]byte) error) error
	Close() error
}

type Queue[T any] struct {
	topic  string
	broker Broker
	mu     sync.RWMutex
	closed bool
	opts   *options
}

func New[T any](broker Broker, opts ...Option) (*Queue[T], error) {
	var t T
	typ := reflect.TypeOf(t)

	if typ.Kind() != reflect.Ptr || typ.Elem().Kind() != reflect.Struct {
		return nil, ErrInvalidJobType
	}

	queue := &Queue[T]{
		topic:  typ.String(),
		broker: broker,
		mu:     sync.RWMutex{},
		closed: false,
		opts: &options{
			panicHandler: newDefaultPanicHandler(),
			errorHandler: newDefaultErrorHandler(),
			workerCount:  runtime.NumCPU(),
			maxRetries:   3,
			backoff: FixedBackoff{
				Duration: 1 * time.Second,
			},
			prefix:     "",
			dlqEnabled: false,
		},
	}
	for _, opt := range opts {
		opt(queue.opts)
	}

	return queue, nil
}

func (q *Queue[T]) Produce(ctx context.Context, job T) error {
	q.mu.RLock()
	if q.closed {
		q.mu.RUnlock()
		return ErrQueueClosed
	}
	q.mu.RUnlock()

	data, err := json.Marshal(job)
	if err != nil {
		return err
	}

	topic := q.getPrefixedTopic()
	return q.broker.Produce(ctx, topic, data)
}

func (q *Queue[T]) Consume(ctx context.Context, handler func(context.Context, T) error) error {
	jobs := make(chan []byte, q.opts.workerCount*10)
	workerCtx, workerCancel := context.WithCancel(ctx)
	defer workerCancel()

	var workerWg sync.WaitGroup
	for i := 0; i < q.opts.workerCount; i++ {
		workerWg.Add(1)
		go func() {
			defer workerWg.Done()
			for {
				select {
				case data, ok := <-jobs:
					if !ok {
						return
					}
					q.processJob(workerCtx, data, handler)
				case <-workerCtx.Done():
					return
				}
			}
		}()
	}

	defer func() {
		close(jobs)
		workerCancel()
		workerWg.Wait()
	}()

	topic := q.getPrefixedTopic()
	if err := q.broker.Consume(ctx, topic, func(data []byte) error {
		select {
		case jobs <- data:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}); err != nil {
		return err
	}

	<-ctx.Done()

	return ctx.Err()
}

func (q *Queue[T]) Close() error {
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return nil
	}
	q.closed = true
	q.mu.Unlock()

	return q.broker.Close()
}

func (q *Queue[T]) processJob(ctx context.Context, data []byte, handler func(context.Context, T) error) {
	defer func() {
		if r := recover(); r != nil {
			q.opts.panicHandler.Handle(nil, handler, r, debug.Stack())
		}
	}()

	select {
	case <-ctx.Done():
		return
	default:
	}

	retry := 0
	for {
		var job T
		if err := json.Unmarshal(data, &job); err != nil {
			q.opts.errorHandler.Handle(job, handler, err)
			return
		}

		err := handler(ctx, job)
		if err == nil {
			return
		}

		q.opts.errorHandler.Handle(job, handler, err)

		retry++
		if retry > q.opts.maxRetries {
			if q.opts.dlqEnabled {
				q.sendToDLQ(ctx, job)
			}
			return
		}

		delay := q.opts.backoff.Delay(retry)
		if delay > 0 {
			timer := time.NewTimer(delay)
			select {
			case <-timer.C:
			case <-ctx.Done():
				if !timer.Stop() {
					<-timer.C
				}
				return
			}
		}
	}
}

func (q *Queue[T]) sendToDLQ(ctx context.Context, job T) {
	data, err := json.Marshal(job)
	if err != nil {
		q.opts.errorHandler.Handle(job, nil, ErrMarshal)
		return
	}

	dlqCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	topic := q.getDLQTopic()
	if err := q.broker.Produce(dlqCtx, topic, data); err != nil {
		q.opts.errorHandler.Handle(job, nil, ErrSendToDLQ)
	}
}

func (q *Queue[T]) getPrefixedTopic() string {
	if q.opts.prefix == "" {
		return q.topic
	}
	return q.opts.prefix + q.topic
}

func (q *Queue[T]) getDLQTopic() string {
	dlqTopic := "dlq:" + q.topic
	if q.opts.prefix == "" {
		return dlqTopic
	}
	return q.opts.prefix + dlqTopic
}
