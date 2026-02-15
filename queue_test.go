package queue

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type mockBroker struct {
	mu       sync.Mutex
	channels map[string]chan []byte
	done     chan struct{}
	wg       sync.WaitGroup
	closed   bool
}

func newMockBroker() *mockBroker {
	return &mockBroker{
		channels: make(map[string]chan []byte),
		done:     make(chan struct{}),
	}
}

func (b *mockBroker) getOrCreateChan(topic string) chan []byte {
	b.mu.Lock()
	defer b.mu.Unlock()
	if ch, ok := b.channels[topic]; ok {
		return ch
	}
	ch := make(chan []byte, 100)
	b.channels[topic] = ch
	return ch
}

func (b *mockBroker) isClosed() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.closed
}

func (b *mockBroker) Produce(ctx context.Context, topic string, data []byte) error {
	if b.isClosed() {
		return ErrBrokerClosed
	}
	ch := b.getOrCreateChan(topic)
	select {
	case ch <- data:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (b *mockBroker) Consume(
	ctx context.Context, topic string, handler func([]byte) error,
) error {
	if b.isClosed() {
		return ErrBrokerClosed
	}
	ch := b.getOrCreateChan(topic)

	goroutineDone := make(chan struct{})
	b.wg.Add(1)
	go func() {
		defer b.wg.Done()
		defer close(goroutineDone)
		for {
			select {
			case data, ok := <-ch:
				if !ok {
					return
				}
				_ = handler(data)
			case <-ctx.Done():
				return
			case <-b.done:
				return
			}
		}
	}()

	select {
	case <-ctx.Done():
		<-goroutineDone
		return ctx.Err()
	case <-b.done:
		<-goroutineDone
		return ErrBrokerClosed
	}
}

func (b *mockBroker) Ping(_ context.Context) error {
	if b.isClosed() {
		return ErrBrokerClosed
	}
	return nil
}

func (b *mockBroker) Close() error {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil
	}
	b.closed = true
	close(b.done)
	b.mu.Unlock()
	b.wg.Wait()
	b.mu.Lock()
	for topic, ch := range b.channels {
		close(ch)
		delete(b.channels, topic)
	}
	b.mu.Unlock()
	return nil
}

type testJob struct {
	Msg string `json:"msg"`
}

func newQueue(t *testing.T, opts ...Option) (*Queue[*testJob], *mockBroker) {
	t.Helper()
	b := newMockBroker()
	q, err := New[*testJob](b, opts...)
	if err != nil {
		t.Fatalf("failed to create queue: %v", err)
	}
	t.Cleanup(func() {
		q.Close()
		b.Close()
	})
	return q, b
}

func mustMarshalJob(j *testJob) []byte {
	s := JSONSerializer{}
	data, err := s.Marshal(j)
	if err != nil {
		panic(err)
	}
	return data
}

type errorHandlerFunc func(ErrorContext)

func (f errorHandlerFunc) Handle(ctx ErrorContext) { f(ctx) }

type panicHandlerFunc func(PanicContext)

func (f panicHandlerFunc) Handle(ctx PanicContext) { f(ctx) }

func TestNew_ValidType(t *testing.T) {
	t.Parallel()
	q, _ := newQueue(t)
	if q == nil {
		t.Fatal("expected non-nil queue")
	}
}

func TestNew_InvalidType_NotPointer(t *testing.T) {
	t.Parallel()
	_, err := New[int](newMockBroker())
	if !errors.Is(err, ErrInvalidJobType) {
		t.Errorf("expected ErrInvalidJobType, got %v", err)
	}
}

func TestNew_InvalidType_PointerToNonStruct(t *testing.T) {
	t.Parallel()
	_, err := New[*int](newMockBroker())
	if !errors.Is(err, ErrInvalidJobType) {
		t.Errorf("expected ErrInvalidJobType, got %v", err)
	}
}

func TestNew_InvalidType_Interface(t *testing.T) {
	t.Parallel()
	_, err := New[any](newMockBroker())
	if !errors.Is(err, ErrInvalidJobType) {
		t.Errorf("expected ErrInvalidJobType, got %v", err)
	}
}

func TestNew_InvalidOptions_NilBackoff(t *testing.T) {
	t.Parallel()
	_, err := New[*testJob](newMockBroker(), WithBackoff(nil))
	if !errors.Is(err, ErrNilBackoff) {
		t.Errorf("expected ErrNilBackoff, got %v", err)
	}
}

func TestQueue_Produce_BasicSuccess(t *testing.T) {
	t.Parallel()
	q, _ := newQueue(t)
	err := q.Produce(context.Background(), &testJob{Msg: "hello"})
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
}

func TestQueue_Produce_WithHeaders(t *testing.T) {
	t.Parallel()
	q, _ := newQueue(t)
	err := q.Produce(context.Background(), &testJob{Msg: "hi"},
		WithHeaders(map[string]string{"x": "y"}))
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
}

func TestQueue_Produce_ClosedQueue(t *testing.T) {
	t.Parallel()
	b := newMockBroker()
	q, _ := New[*testJob](b)
	q.Close()
	err := q.Produce(context.Background(), &testJob{Msg: "nope"})
	if !errors.Is(err, ErrQueueClosed) {
		t.Errorf("expected ErrQueueClosed, got %v", err)
	}
	b.Close()
}

func TestQueue_Consume_NilHandler(t *testing.T) {
	t.Parallel()
	q, _ := newQueue(t)
	err := q.Consume(context.Background(), nil)
	if !errors.Is(err, ErrNilHandler) {
		t.Errorf("expected ErrNilHandler, got %v", err)
	}
}

func TestQueue_Consume_ClosedQueue(t *testing.T) {
	t.Parallel()
	b := newMockBroker()
	q, _ := New[*testJob](b)
	q.Close()
	err := q.Consume(context.Background(), func(_ context.Context, _ *testJob) error {
		return nil
	})
	if !errors.Is(err, ErrQueueClosed) {
		t.Errorf("expected ErrQueueClosed, got %v", err)
	}
	b.Close()
}

func TestQueue_ProduceConsume_EndToEnd(t *testing.T) {
	t.Parallel()
	q, _ := newQueue(t, WithWorkerCount(1), WithBackoff(NoBackoff{}))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var got atomic.Value
	doneCh := make(chan struct{})
	go func() {
		_ = q.Consume(ctx, func(_ context.Context, job *testJob) error {
			got.Store(job.Msg)
			cancel()
			return nil
		})
		close(doneCh)
	}()

	time.Sleep(50 * time.Millisecond)
	if err := q.Produce(ctx, &testJob{Msg: "e2e"}); err != nil {
		t.Fatalf("produce error: %v", err)
	}
	<-doneCh
	if v, _ := got.Load().(string); v != "e2e" {
		t.Errorf("expected 'e2e', got %q", v)
	}
}

func TestQueue_Close_Idempotent(t *testing.T) {
	t.Parallel()
	b := newMockBroker()
	q, _ := New[*testJob](b)
	if err := q.Close(); err != nil {
		t.Errorf("first close: %v", err)
	}
	if err := q.Close(); err != nil {
		t.Errorf("second close: %v", err)
	}
	b.Close()
}

func TestQueue_Topic_Default(t *testing.T) {
	t.Parallel()
	q, _ := newQueue(t)
	if q.Topic() == "" {
		t.Error("expected non-empty topic")
	}
}

func TestQueue_Topic_WithPrefixAndCustomTopic(t *testing.T) {
	t.Parallel()
	q, _ := newQueue(t, WithPrefix("svc:"), WithTopic("custom"))
	if q.Topic() != "svc:custom" {
		t.Errorf("expected 'svc:custom', got %q", q.Topic())
	}
}

func TestQueue_DLQTopic_Default(t *testing.T) {
	t.Parallel()
	q, _ := newQueue(t)
	dlq := q.DLQTopic()
	if dlq == "" || len(dlq) < len(dlqTopicPrefix) {
		t.Errorf("expected DLQ topic with prefix, got %q", dlq)
	}
}

func TestQueue_DLQTopic_WithPrefixAndCustomTopic(t *testing.T) {
	t.Parallel()
	q, _ := newQueue(t, WithPrefix("svc:"), WithTopic("custom"))
	want := "svc:" + dlqTopicPrefix + "custom"
	if q.DLQTopic() != want {
		t.Errorf("expected %q, got %q", want, q.DLQTopic())
	}
}

func TestQueue_Use_Middleware(t *testing.T) {
	t.Parallel()
	q, _ := newQueue(t, WithWorkerCount(1), WithBackoff(NoBackoff{}))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var mwCalled atomic.Bool
	q.Use(func(next func(context.Context, *testJob) error) func(context.Context, *testJob) error {
		return func(ctx context.Context, j *testJob) error {
			mwCalled.Store(true)
			return next(ctx, j)
		}
	})

	doneCh := make(chan struct{})
	go func() {
		_ = q.Consume(ctx, func(_ context.Context, _ *testJob) error {
			cancel()
			return nil
		})
		close(doneCh)
	}()

	time.Sleep(50 * time.Millisecond)
	_ = q.Produce(ctx, &testJob{Msg: "mw"})
	<-doneCh
	if !mwCalled.Load() {
		t.Error("expected middleware to be called")
	}
}

func TestQueue_Ping(t *testing.T) {
	t.Parallel()
	q, _ := newQueue(t)
	if err := q.Ping(context.Background()); err != nil {
		t.Errorf("expected nil, got %v", err)
	}
}

func TestQueue_RetryOnError(t *testing.T) {
	t.Parallel()
	q, _ := newQueue(t,
		WithWorkerCount(1),
		WithMaxRetries(2),
		WithBackoff(NoBackoff{}),
	)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var attempts atomic.Int32
	doneCh := make(chan struct{})
	go func() {
		_ = q.Consume(ctx, func(_ context.Context, _ *testJob) error {
			attempts.Add(1)
			return fmt.Errorf("fail")
		})
		close(doneCh)
	}()

	time.Sleep(50 * time.Millisecond)
	_ = q.Produce(ctx, &testJob{Msg: "retry"})
	time.Sleep(300 * time.Millisecond)
	cancel()
	<-doneCh

	got := int(attempts.Load())
	if got != 3 {
		t.Errorf("expected 3 attempts (1 + 2 retries), got %d", got)
	}
}

func TestQueue_DLQ_SendsAfterRetries(t *testing.T) {
	t.Parallel()
	b := newMockBroker()
	q, err := New[*testJob](b,
		WithWorkerCount(1),
		WithMaxRetries(1),
		WithBackoff(NoBackoff{}),
		WithDLQ(true),
	)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var attempts atomic.Int32
	doneCh := make(chan struct{})
	go func() {
		_ = q.Consume(ctx, func(_ context.Context, _ *testJob) error {
			attempts.Add(1)
			return fmt.Errorf("always fail")
		})
		close(doneCh)
	}()

	time.Sleep(50 * time.Millisecond)
	_ = q.Produce(ctx, &testJob{Msg: "dlq"})
	time.Sleep(300 * time.Millisecond)
	cancel()
	<-doneCh
	q.Close()
	b.Close()

	if attempts.Load() < 2 {
		t.Errorf("expected at least 2 attempts, got %d", attempts.Load())
	}
}

func TestQueue_PanicInHandler(t *testing.T) {
	t.Parallel()
	b := newMockBroker()
	q, err := New[*testJob](b,
		WithWorkerCount(1),
		WithMaxRetries(0),
		WithBackoff(NoBackoff{}),
	)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	doneCh := make(chan struct{})
	go func() {
		_ = q.Consume(ctx, func(_ context.Context, _ *testJob) error {
			panic("handler panic")
		})
		close(doneCh)
	}()

	time.Sleep(50 * time.Millisecond)
	_ = q.Produce(ctx, &testJob{Msg: "panic"})
	time.Sleep(200 * time.Millisecond)
	cancel()
	<-doneCh
	q.Close()
	b.Close()
}

func TestQueue_WaitBackoff_ContextCancelled(t *testing.T) {
	t.Parallel()
	q, _ := newQueue(t, WithBackoff(FixedBackoff{Duration: 10 * time.Second}))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if q.waitBackoff(ctx, 0) {
		t.Error("expected false when context is cancelled")
	}
}

func TestQueue_WaitBackoff_ZeroDelay(t *testing.T) {
	t.Parallel()
	q, _ := newQueue(t, WithBackoff(NoBackoff{}))
	if !q.waitBackoff(context.Background(), 0) {
		t.Error("expected true for zero delay")
	}
}

func TestQueue_DecodeJob_InvalidEnvelope(t *testing.T) {
	t.Parallel()
	q, _ := newQueue(t)
	_, _, err := q.decodeJob([]byte("not json"))
	if err == nil {
		t.Error("expected error for invalid envelope")
	}
}

func TestQueue_DecodeJob_InvalidJobData(t *testing.T) {
	t.Parallel()
	q, _ := newQueue(t)
	env := newEnvelope("t", []byte("not a job"), nil)
	data, _ := marshalEnvelope(env)
	_, _, err := q.decodeJob(data)
	if err == nil {
		t.Error("expected error for invalid job data")
	}
}

func TestQueue_ProcessJob_UnmarshalError(t *testing.T) {
	t.Parallel()
	q, _ := newQueue(t)
	var errReported atomic.Bool
	q.opts.errorHandler = errorHandlerFunc(func(ec ErrorContext) {
		if errors.Is(ec.Err, ErrUnmarshal) {
			errReported.Store(true)
		}
	})
	q.processJob(context.Background(), []byte("bad"),
		func(_ context.Context, _ *testJob) error { return nil })
	if !errReported.Load() {
		t.Error("expected ErrUnmarshal to be reported")
	}
}

func TestQueue_EnqueueJob_ContextDone(t *testing.T) {
	t.Parallel()
	q, _ := newQueue(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	jobs := make(chan []byte)
	fn := q.enqueueJob(ctx, jobs)
	if err := fn([]byte("data")); err == nil {
		t.Error("expected context error")
	}
}

func TestQueue_MergeContext_QueueCancelPropagates(t *testing.T) {
	t.Parallel()
	b := newMockBroker()
	q, _ := New[*testJob](b)
	merged, mergedCancel := q.mergeContext(context.Background())
	defer mergedCancel()
	q.Close()
	b.Close()
	select {
	case <-merged.Done():
	case <-time.After(time.Second):
		t.Error("expected merged context to be cancelled")
	}
}

func TestQueue_StartWorkers_ProcessMessages(t *testing.T) {
	t.Parallel()
	q, _ := newQueue(t, WithWorkerCount(2), WithBackoff(NoBackoff{}))
	jobs := make(chan []byte, 2)

	env := newEnvelope(q.Topic(), mustMarshalJob(&testJob{Msg: "w"}), nil)
	data, _ := marshalEnvelope(env)
	jobs <- data

	var called atomic.Bool
	wg := q.startWorkers(context.Background(), jobs,
		func(_ context.Context, _ *testJob) error {
			called.Store(true)
			return nil
		})
	time.Sleep(50 * time.Millisecond)
	close(jobs)
	wg.Wait()
	if !called.Load() {
		t.Error("expected handler to be called")
	}
}

func TestQueue_SafeExecute_NormalReturn(t *testing.T) {
	t.Parallel()
	q, _ := newQueue(t)
	env := newEnvelope("t", nil, nil)
	err := q.safeExecute(context.Background(), env, &testJob{},
		func(_ context.Context, _ *testJob) error { return nil })
	if err != nil {
		t.Errorf("expected nil, got %v", err)
	}
}

func TestQueue_SafeExecute_ErrorReturn(t *testing.T) {
	t.Parallel()
	q, _ := newQueue(t)
	env := newEnvelope("t", nil, nil)
	err := q.safeExecute(context.Background(), env, &testJob{},
		func(_ context.Context, _ *testJob) error { return fmt.Errorf("fail") })
	if err == nil || err.Error() != "fail" {
		t.Errorf("expected 'fail', got %v", err)
	}
}

func TestQueue_SafeExecute_PanicRecover(t *testing.T) {
	t.Parallel()
	q, _ := newQueue(t)
	env := newEnvelope("t", nil, nil)
	var panicHandled atomic.Bool
	q.opts.panicHandler = panicHandlerFunc(func(_ PanicContext) {
		panicHandled.Store(true)
	})
	err := q.safeExecute(context.Background(), env, &testJob{},
		func(_ context.Context, _ *testJob) error { panic("boom") })
	if err == nil {
		t.Error("expected error from recovered panic")
	}
	if !panicHandled.Load() {
		t.Error("expected panic handler to be called")
	}
}

func TestQueue_BuildMeta(t *testing.T) {
	t.Parallel()
	q, _ := newQueue(t)
	env := &Envelope{
		ID: "id-123", Topic: "orders", Attempt: 2,
		Headers: map[string]string{"a": "b"},
	}
	meta := q.buildMeta(env, 1)
	if meta.ID != "id-123" {
		t.Errorf("expected ID 'id-123', got %q", meta.ID)
	}
	if meta.Attempt != 3 {
		t.Errorf("expected attempt 3, got %d", meta.Attempt)
	}
}

func TestQueue_ExecuteWithRetries_SuccessFirstTry(t *testing.T) {
	t.Parallel()
	q, _ := newQueue(t, WithMaxRetries(3), WithBackoff(NoBackoff{}))
	env := newEnvelope("t", nil, nil)
	var count atomic.Int32
	q.executeWithRetries(context.Background(), env, &testJob{},
		func(_ context.Context, _ *testJob) error {
			count.Add(1)
			return nil
		})
	if count.Load() != 1 {
		t.Errorf("expected 1 call, got %d", count.Load())
	}
}

func TestQueue_ExecuteWithRetries_FailAndDLQ(t *testing.T) {
	t.Parallel()
	b := newMockBroker()
	q, _ := New[*testJob](b,
		WithMaxRetries(1), WithBackoff(NoBackoff{}), WithDLQ(true),
	)
	defer func() { q.Close(); b.Close() }()

	env := newEnvelope(q.Topic(), nil, nil)
	var mu sync.Mutex
	var count int
	q.executeWithRetries(context.Background(), env, &testJob{},
		func(_ context.Context, _ *testJob) error {
			mu.Lock()
			count++
			mu.Unlock()
			return fmt.Errorf("fail")
		})
	mu.Lock()
	defer mu.Unlock()
	if count != 2 {
		t.Errorf("expected 2 attempts, got %d", count)
	}
}

func TestQueue_GetTopicName_AllVariants(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name   string
		opts   []Option
		expect string
	}{
		{"default", nil, "queue.testJob"},
		{"custom_topic", []Option{WithTopic("override")}, "override"},
		{"with_prefix", []Option{WithPrefix("ns:")}, "ns:queue.testJob"},
		{"prefix_and_topic", []Option{WithPrefix("ns:"), WithTopic("x")}, "ns:x"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			q, _ := newQueue(t, tc.opts...)
			if got := q.getTopicName(); got != tc.expect {
				t.Errorf("expected %q, got %q", tc.expect, got)
			}
		})
	}
}

func TestQueue_GetDLQTopic_AllVariants(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name   string
		opts   []Option
		expect string
	}{
		{"default", nil, "dlq:queue.testJob"},
		{"custom", []Option{WithTopic("x")}, "dlq:x"},
		{"prefix", []Option{WithPrefix("ns:"), WithTopic("x")}, "ns:dlq:x"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			q, _ := newQueue(t, tc.opts...)
			if got := q.getDLQTopic(); got != tc.expect {
				t.Errorf("expected %q, got %q", tc.expect, got)
			}
		})
	}
}
