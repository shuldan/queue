package queue

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type TestJob struct {
	ID   int
	Name string
}

type mockBroker struct {
	produceFunc func(ctx context.Context, topic string, data []byte) error
	consumeFunc func(ctx context.Context, topic string, handler func([]byte) error) error
	closeFunc   func() error
}

func (m *mockBroker) Produce(ctx context.Context, topic string, data []byte) error {
	if m.produceFunc != nil {
		return m.produceFunc(ctx, topic, data)
	}
	return nil
}

func (m *mockBroker) Consume(ctx context.Context, topic string, handler func([]byte) error) error {
	if m.consumeFunc != nil {
		return m.consumeFunc(ctx, topic, handler)
	}
	return nil
}

func (m *mockBroker) Close() error {
	if m.closeFunc != nil {
		return m.closeFunc()
	}
	return nil
}

func TestNew_InvalidJobType_NonPointer(t *testing.T) {
	t.Parallel()

	type NotAPointer struct {
		ID int
	}

	broker := &mockBroker{}
	_, err := New[NotAPointer](broker)

	if !errors.Is(err, ErrInvalidJobType) {
		t.Errorf("expected ErrInvalidJobType, got %v", err)
	}
}

func TestNew_InvalidJobType_PointerToNonStruct(t *testing.T) {
	t.Parallel()

	broker := &mockBroker{}
	_, err := New[*int](broker)

	if !errors.Is(err, ErrInvalidJobType) {
		t.Errorf("expected ErrInvalidJobType, got %v", err)
	}
}

func TestNew_Success(t *testing.T) {
	t.Parallel()

	broker := &mockBroker{}
	q, err := New[*TestJob](broker)

	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
	if q == nil {
		t.Error("expected non-nil queue")
	}
	if q != nil && q.broker != broker {
		t.Error("broker not set correctly")
	}
}

func TestNew_WithOptions(t *testing.T) {
	t.Parallel()

	broker := &mockBroker{}
	customBackoff := NoBackoff{}

	q, err := New[*TestJob](broker,
		WithPrefix("test:"),
		WithDLQ(true),
		WithWorkerCount(5),
		WithMaxRetries(10),
		WithBackoff(customBackoff),
	)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if q.opts.prefix != "test:" {
		t.Errorf("expected prefix 'test:', got %q", q.opts.prefix)
	}
	if !q.opts.dlqEnabled {
		t.Error("expected DLQ enabled")
	}
	if q.opts.workerCount != 5 {
		t.Errorf("expected 5 workers, got %d", q.opts.workerCount)
	}
	if q.opts.maxRetries != 10 {
		t.Errorf("expected 10 retries, got %d", q.opts.maxRetries)
	}
}

func TestQueue_Produce_QueueClosed(t *testing.T) {
	t.Parallel()

	broker := &mockBroker{}
	q, _ := New[*TestJob](broker)
	q.closed = true

	err := q.Produce(context.Background(), &TestJob{ID: 1})

	if !errors.Is(err, ErrQueueClosed) {
		t.Errorf("expected ErrQueueClosed, got %v", err)
	}
}

func TestQueue_Produce_Success(t *testing.T) {
	t.Parallel()

	var capturedTopic string
	var capturedData []byte

	broker := &mockBroker{
		produceFunc: func(ctx context.Context, topic string, data []byte) error {
			capturedTopic = topic
			capturedData = data
			return nil
		},
	}

	q, _ := New[*TestJob](broker)
	job := &TestJob{ID: 42, Name: "test"}

	err := q.Produce(context.Background(), job)

	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if capturedTopic == "" {
		t.Error("topic not passed to broker")
	}
	if len(capturedData) == 0 {
		t.Error("data not passed to broker")
	}
}

func TestQueue_Produce_BrokerError(t *testing.T) {
	t.Parallel()

	expectedErr := errors.New("broker error")
	broker := &mockBroker{
		produceFunc: func(ctx context.Context, topic string, data []byte) error {
			return expectedErr
		},
	}

	q, _ := New[*TestJob](broker)
	err := q.Produce(context.Background(), &TestJob{ID: 1})

	if !errors.Is(err, expectedErr) {
		t.Errorf("expected broker error, got %v", err)
	}
}

func TestQueue_Close_AlreadyClosed(t *testing.T) {
	t.Parallel()

	broker := &mockBroker{}
	q, _ := New[*TestJob](broker)
	q.closed = true

	err := q.Close()
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
}

func TestQueue_Close_Success(t *testing.T) {
	t.Parallel()

	closeCalled := false
	broker := &mockBroker{
		closeFunc: func() error {
			closeCalled = true
			return nil
		},
	}

	q, _ := New[*TestJob](broker)
	err := q.Close()

	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if !closeCalled {
		t.Error("broker.Close not called")
	}
	if !q.closed {
		t.Error("queue not marked as closed")
	}
}

func TestQueue_Close_BrokerError(t *testing.T) {
	t.Parallel()

	expectedErr := errors.New("close error")
	broker := &mockBroker{
		closeFunc: func() error {
			return expectedErr
		},
	}

	q, _ := New[*TestJob](broker)
	err := q.Close()

	if !errors.Is(err, expectedErr) {
		t.Errorf("expected close error, got %v", err)
	}
}

func TestQueue_GetPrefixedTopic_NoPrefix(t *testing.T) {
	t.Parallel()

	broker := &mockBroker{}
	q, _ := New[*TestJob](broker)

	topic := q.getPrefixedTopic()
	expectedType := reflect.TypeOf((*TestJob)(nil)).String()

	if topic != expectedType {
		t.Errorf("expected %q, got %q", expectedType, topic)
	}
}

func TestQueue_GetPrefixedTopic_WithPrefix(t *testing.T) {
	t.Parallel()

	broker := &mockBroker{}
	q, _ := New[*TestJob](broker, WithPrefix("myprefix:"))

	topic := q.getPrefixedTopic()
	expectedType := reflect.TypeOf((*TestJob)(nil)).String()
	expected := "myprefix:" + expectedType

	if topic != expected {
		t.Errorf("expected %q, got %q", expected, topic)
	}
}

func TestQueue_GetDLQTopic_NoPrefix(t *testing.T) {
	t.Parallel()

	broker := &mockBroker{}
	q, _ := New[*TestJob](broker)

	dlqTopic := q.getDLQTopic()
	expectedType := reflect.TypeOf((*TestJob)(nil)).String()
	expected := "dlq:" + expectedType

	if dlqTopic != expected {
		t.Errorf("expected %q, got %q", expected, dlqTopic)
	}
}

func TestQueue_GetDLQTopic_WithPrefix(t *testing.T) {
	t.Parallel()

	broker := &mockBroker{}
	q, _ := New[*TestJob](broker, WithPrefix("myprefix:"))

	dlqTopic := q.getDLQTopic()
	expectedType := reflect.TypeOf((*TestJob)(nil)).String()
	expected := "myprefix:dlq:" + expectedType

	if dlqTopic != expected {
		t.Errorf("expected %q, got %q", expected, dlqTopic)
	}
}

func TestQueue_Consume_BrokerError(t *testing.T) {
	expectedErr := errors.New("consume error")
	broker := &mockBroker{
		consumeFunc: func(ctx context.Context, topic string, handler func([]byte) error) error {
			return expectedErr
		},
	}

	q, _ := New[*TestJob](broker)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := q.Consume(ctx, func(ctx context.Context, job *TestJob) error {
		return nil
	})

	if !errors.Is(err, expectedErr) {
		t.Errorf("expected consume error, got %v", err)
	}
}

type mockErrorHandler struct {
	mu     sync.Mutex
	errors []error
}

func (m *mockErrorHandler) Handle(_ any, _ any, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.errors = append(m.errors, err)
}

type mockPanicHandler struct {
	mu     sync.Mutex
	panics []any
}

func (m *mockPanicHandler) Handle(_ any, _ any, panicValue any, _ []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.panics = append(m.panics, panicValue)
}

func TestQueue_ProcessJob_UnmarshalError(t *testing.T) {
	t.Parallel()

	errHandler := &mockErrorHandler{}
	broker := &mockBroker{}
	q, _ := New[*TestJob](broker, WithErrorHandler(errHandler))

	invalidJSON := []byte("{invalid json}")
	handler := func(ctx context.Context, job *TestJob) error {
		return nil
	}

	q.processJob(context.Background(), invalidJSON, handler)

	if len(errHandler.errors) == 0 {
		t.Error("expected error to be handled")
	}
}

func TestQueue_ProcessJob_HandlerPanic(t *testing.T) {
	t.Parallel()

	panicHandler := &mockPanicHandler{}
	broker := &mockBroker{}
	q, _ := New[*TestJob](broker, WithPanicHandler(panicHandler))

	data := []byte(`{"ID":1,"Name":"test"}`)
	handler := func(ctx context.Context, job *TestJob) error {
		panic("test panic")
	}

	q.processJob(context.Background(), data, handler)

	time.Sleep(10 * time.Millisecond)

	if len(panicHandler.panics) == 0 {
		t.Error("expected panic to be handled")
	}
}

func TestQueue_ProcessJob_ContextCancelled(t *testing.T) {
	t.Parallel()

	broker := &mockBroker{}
	q, _ := New[*TestJob](broker)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	data := []byte(`{"ID":1,"Name":"test"}`)
	called := false
	handler := func(ctx context.Context, job *TestJob) error {
		called = true
		return nil
	}

	q.processJob(ctx, data, handler)

	if called {
		t.Error("handler should not be called when context is cancelled")
	}
}

func TestQueue_ProcessJob_Success(t *testing.T) {
	t.Parallel()

	broker := &mockBroker{}
	q, _ := New[*TestJob](broker)

	data := []byte(`{"ID":42,"Name":"test"}`)
	var receivedJob *TestJob
	handler := func(ctx context.Context, job *TestJob) error {
		receivedJob = job
		return nil
	}

	q.processJob(context.Background(), data, handler)

	if receivedJob == nil {
		t.Fatal("handler not called")
	}
	if receivedJob.ID != 42 || receivedJob.Name != "test" {
		t.Errorf("unexpected job: %+v", receivedJob)
	}
}

func TestQueue_ProcessJob_RetrySuccess(t *testing.T) {
	broker := &mockBroker{}
	q, _ := New[*TestJob](broker, WithMaxRetries(2), WithBackoff(NoBackoff{}))

	data := []byte(`{"ID":1,"Name":"test"}`)
	attempts := 0
	handler := func(ctx context.Context, job *TestJob) error {
		attempts++
		if attempts < 2 {
			return errors.New("temporary error")
		}
		return nil
	}

	q.processJob(context.Background(), data, handler)

	if attempts != 2 {
		t.Errorf("expected 2 attempts, got %d", attempts)
	}
}

func TestQueue_ProcessJob_RetriesExhausted_NoDLQ(t *testing.T) {
	errHandler := &mockErrorHandler{}
	broker := &mockBroker{}
	q, _ := New[*TestJob](broker, WithMaxRetries(1), WithBackoff(NoBackoff{}), WithErrorHandler(errHandler))

	data := []byte(`{"ID":1,"Name":"test"}`)
	handler := func(ctx context.Context, job *TestJob) error {
		return errors.New("permanent error")
	}

	q.processJob(context.Background(), data, handler)

	if len(errHandler.errors) != 2 {
		t.Errorf("expected 2 error calls, got %d", len(errHandler.errors))
	}
}

func TestQueue_ProcessJob_RetriesExhausted_WithDLQ(t *testing.T) {
	var dlqData []byte
	broker := &mockBroker{
		produceFunc: func(ctx context.Context, topic string, data []byte) error {
			dlqData = data
			return nil
		},
	}
	q, _ := New[*TestJob](broker, WithMaxRetries(1), WithBackoff(NoBackoff{}), WithDLQ(true))

	data := []byte(`{"ID":1,"Name":"test"}`)
	handler := func(ctx context.Context, job *TestJob) error {
		return errors.New("permanent error")
	}

	q.processJob(context.Background(), data, handler)

	time.Sleep(10 * time.Millisecond)

	if len(dlqData) == 0 {
		t.Error("expected job to be sent to DLQ")
	}
}

func TestQueue_SendToDLQ_MarshalError(t *testing.T) {
	t.Parallel()

	type UnmarshallableJob struct {
		Ch chan int
	}

	errHandler := &mockErrorHandler{}
	broker := &mockBroker{}
	q, _ := New[*UnmarshallableJob](broker, WithErrorHandler(errHandler))

	job := &UnmarshallableJob{Ch: make(chan int)}
	q.sendToDLQ(context.Background(), job)

	if len(errHandler.errors) == 0 {
		t.Error("expected marshal error to be handled")
	}
}

func TestQueue_SendToDLQ_ProduceError(t *testing.T) {
	t.Parallel()

	errHandler := &mockErrorHandler{}
	broker := &mockBroker{
		produceFunc: func(ctx context.Context, topic string, data []byte) error {
			return errors.New("produce error")
		},
	}
	q, _ := New[*TestJob](broker, WithErrorHandler(errHandler))

	job := &TestJob{ID: 1, Name: "test"}
	q.sendToDLQ(context.Background(), job)

	time.Sleep(10 * time.Millisecond)

	found := false
	errHandler.mu.Lock()
	for _, err := range errHandler.errors {
		if errors.Is(err, ErrSendToDLQ) {
			found = true
			break
		}
	}
	errHandler.mu.Unlock()

	if !found {
		t.Error("expected ErrSendToDLQ to be handled")
	}
}

func TestQueue_ProcessJob_RetryWithDelay(t *testing.T) {
	broker := &mockBroker{}
	backoff := FixedBackoff{Duration: 10 * time.Millisecond}
	q, _ := New[*TestJob](broker, WithMaxRetries(1), WithBackoff(backoff))

	data := []byte(`{"ID":1,"Name":"test"}`)
	start := time.Now()
	attempts := 0

	handler := func(ctx context.Context, job *TestJob) error {
		attempts++
		if attempts == 1 {
			return errors.New("retry")
		}
		return nil
	}

	q.processJob(context.Background(), data, handler)
	elapsed := time.Since(start)

	if elapsed < 10*time.Millisecond {
		t.Error("expected delay before retry")
	}
	if attempts != 2 {
		t.Errorf("expected 2 attempts, got %d", attempts)
	}
}

func TestQueue_ProcessJob_CancelledDuringDelay(t *testing.T) {
	broker := &mockBroker{}
	backoff := FixedBackoff{Duration: 100 * time.Millisecond}
	q, _ := New[*TestJob](broker, WithMaxRetries(5), WithBackoff(backoff))

	ctx, cancel := context.WithCancel(context.Background())

	data := []byte(`{"ID":1,"Name":"test"}`)
	attempts := 0

	handler := func(ctx context.Context, job *TestJob) error {
		attempts++
		if attempts == 1 {
			go func() {
				time.Sleep(10 * time.Millisecond)
				cancel()
			}()
			return errors.New("retry")
		}
		return nil
	}

	q.processJob(ctx, data, handler)

	if attempts > 2 {
		t.Errorf("expected at most 2 attempts due to cancellation, got %d", attempts)
	}
}

type JobWithUnmarshallableField struct {
	ID      int
	Channel chan int
}

func TestQueue_Produce_MarshalError(t *testing.T) {
	t.Parallel()

	broker := &mockBroker{}
	q, err := New[*JobWithUnmarshallableField](broker)
	if err != nil {
		t.Fatalf("failed to create queue: %v", err)
	}

	job := &JobWithUnmarshallableField{
		ID:      1,
		Channel: make(chan int),
	}

	err = q.Produce(context.Background(), job)
	if err == nil {
		t.Error("expected marshal error, got nil")
	}
}

type JobWithUnmarshallableFunc struct {
	ID   int
	Func func()
}

func TestQueue_Produce_MarshalError_Function(t *testing.T) {
	t.Parallel()

	broker := &mockBroker{}
	q, err := New[*JobWithUnmarshallableFunc](broker)
	if err != nil {
		t.Fatalf("failed to create queue: %v", err)
	}

	job := &JobWithUnmarshallableFunc{
		ID:   1,
		Func: func() {},
	}

	err = q.Produce(context.Background(), job)
	if err == nil {
		t.Error("expected marshal error, got nil")
	}
}

func TestQueue_Consume_WorkersProcessJobs(t *testing.T) {
	var processedCount int32
	jobData := []byte(`{"ID":1,"Name":"test"}`)

	broker := &mockBroker{
		consumeFunc: func(ctx context.Context, topic string, handler func([]byte) error) error {
			for i := 0; i < 10; i++ {
				if err := handler(jobData); err != nil {
					return err
				}
			}
			<-ctx.Done()
			return ctx.Err()
		},
	}

	q, _ := New[*TestJob](broker, WithWorkerCount(3))

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	go func() {
		_ = q.Consume(ctx, func(ctx context.Context, job *TestJob) error {
			atomic.AddInt32(&processedCount, 1)
			return nil
		})
	}()

	time.Sleep(50 * time.Millisecond)
	cancel()
	time.Sleep(10 * time.Millisecond)

	count := atomic.LoadInt32(&processedCount)
	if count != 10 {
		t.Errorf("expected 10 jobs processed, got %d", count)
	}
}

func TestQueue_Consume_ContextCancelledDuringSend(t *testing.T) {
	var contextErrReturned atomic.Bool

	broker := &mockBroker{
		consumeFunc: func(ctx context.Context, topic string, handler func([]byte) error) error {
			for i := 0; i < 1000; i++ {
				select {
				case <-ctx.Done():
					contextErrReturned.Store(true)
					return ctx.Err()
				default:
				}

				err := handler([]byte(`{"ID":1,"Name":"test"}`))
				if err != nil {
					if errors.Is(err, context.Canceled) {
						contextErrReturned.Store(true)
					}
					return err
				}
				time.Sleep(time.Microsecond)
			}
			<-ctx.Done()
			return ctx.Err()
		},
	}

	q, _ := New[*TestJob](broker, WithWorkerCount(1))

	ctx, cancel := context.WithCancel(context.Background())

	consumeDone := make(chan struct{})
	go func() {
		_ = q.Consume(ctx, func(ctx context.Context, job *TestJob) error {
			time.Sleep(time.Millisecond)
			return nil
		})
		close(consumeDone)
	}()

	time.Sleep(5 * time.Millisecond)
	cancel()

	select {
	case <-consumeDone:
	case <-time.After(200 * time.Millisecond):
		t.Error("Consume did not finish after context cancellation")
	}

	if !contextErrReturned.Load() {
		t.Error("expected context error to be returned from handler")
	}
}

func TestQueue_Consume_JobChannelSendSuccess(t *testing.T) {
	jobReceived := make(chan struct{})
	jobData := []byte(`{"ID":42,"Name":"success"}`)

	broker := &mockBroker{
		consumeFunc: func(ctx context.Context, topic string, handler func([]byte) error) error {
			err := handler(jobData)
			if err != nil {
				return err
			}
			<-ctx.Done()
			return ctx.Err()
		},
	}

	q, _ := New[*TestJob](broker, WithWorkerCount(2))

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	go func() {
		_ = q.Consume(ctx, func(ctx context.Context, job *TestJob) error {
			if job.ID == 42 && job.Name == "success" {
				close(jobReceived)
			}
			return nil
		})
	}()

	select {
	case <-jobReceived:
	case <-time.After(50 * time.Millisecond):
		t.Error("job not received within timeout")
	}

	cancel()
}

func TestQueue_Consume_WaitsForContextDone(t *testing.T) {
	consumeStarted := make(chan struct{})
	consumeFinished := make(chan struct{})

	broker := &mockBroker{
		consumeFunc: func(ctx context.Context, topic string, handler func([]byte) error) error {
			close(consumeStarted)
			<-ctx.Done()
			return ctx.Err()
		},
	}

	q, _ := New[*TestJob](broker)

	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		err := q.Consume(ctx, func(ctx context.Context, job *TestJob) error {
			return nil
		})
		if !errors.Is(err, context.Canceled) {
			t.Errorf("expected context.Canceled, got %v", err)
		}
		close(consumeFinished)
	}()

	<-consumeStarted

	cancel()

	select {
	case <-consumeFinished:
	case <-time.After(100 * time.Millisecond):
		t.Error("Consume did not finish after context cancellation")
	}
}

func TestQueue_Consume_ReturnsContextError(t *testing.T) {
	broker := &mockBroker{
		consumeFunc: func(ctx context.Context, topic string, handler func([]byte) error) error {
			<-ctx.Done()
			return ctx.Err()
		},
	}

	q, _ := New[*TestJob](broker)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	err := q.Consume(ctx, func(ctx context.Context, job *TestJob) error {
		return nil
	})

	if err == nil {
		t.Error("expected context error, got nil")
	}

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("expected DeadlineExceeded, got %v", err)
	}
}

func TestQueue_Consume_MultipleWorkersConcurrency(t *testing.T) {
	const numWorkers = 5
	const numJobs = 50

	var mu sync.Mutex
	processedJobs := make(map[int]int)
	jobData := make([][]byte, numJobs)

	for i := 0; i < numJobs; i++ {
		jobData[i] = []byte(`{"ID":` + string(rune('0'+i%10)) + `,"Name":"job"}`)
	}

	broker := &mockBroker{
		consumeFunc: func(ctx context.Context, topic string, handler func([]byte) error) error {
			for _, data := range jobData {
				if err := handler(data); err != nil {
					return err
				}
			}
			<-ctx.Done()
			return ctx.Err()
		},
	}

	q, _ := New[*TestJob](broker, WithWorkerCount(numWorkers))

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	go func() {
		_ = q.Consume(ctx, func(ctx context.Context, job *TestJob) error {
			mu.Lock()
			processedJobs[job.ID]++
			mu.Unlock()
			time.Sleep(time.Millisecond)
			return nil
		})
	}()

	time.Sleep(150 * time.Millisecond)
	cancel()
	time.Sleep(20 * time.Millisecond)

	mu.Lock()
	totalProcessed := 0
	for _, count := range processedJobs {
		totalProcessed += count
	}
	mu.Unlock()

	if totalProcessed != numJobs {
		t.Errorf("expected %d jobs processed, got %d", numJobs, totalProcessed)
	}
}

func TestQueue_Consume_ChannelFullBlocksUntilCancel(t *testing.T) {
	sendAttempts := atomic.Int32{}
	contextCancelDetected := atomic.Bool{}

	broker := &mockBroker{
		consumeFunc: func(ctx context.Context, topic string, handler func([]byte) error) error {
			for i := 0; i < 100; i++ {
				sendAttempts.Add(1)
				err := handler([]byte(`{"ID":1,"Name":"test"}`))
				if err != nil {
					if errors.Is(err, context.Canceled) {
						contextCancelDetected.Store(true)
					}
					return err
				}
			}
			<-ctx.Done()
			return ctx.Err()
		},
	}

	q, _ := New[*TestJob](broker, WithWorkerCount(1))

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	consumeDone := make(chan struct{})
	go func() {
		_ = q.Consume(ctx, func(ctx context.Context, job *TestJob) error {
			time.Sleep(10 * time.Millisecond)
			return nil
		})
		close(consumeDone)
	}()

	<-consumeDone

	if contextCancelDetected.Load() {
		t.Log("context cancellation detected during send - good")
	}
}

func TestQueue_Consume_WorkerContextCancellation(t *testing.T) {
	workerStarted := make(chan struct{})

	broker := &mockBroker{
		consumeFunc: func(ctx context.Context, topic string, handler func([]byte) error) error {
			close(workerStarted)
			<-ctx.Done()
			return ctx.Err()
		},
	}

	q, _ := New[*TestJob](broker, WithWorkerCount(3))

	ctx, cancel := context.WithCancel(context.Background())

	consumeDone := make(chan struct{})
	go func() {
		_ = q.Consume(ctx, func(ctx context.Context, job *TestJob) error {
			return nil
		})
		close(consumeDone)
	}()

	<-workerStarted
	cancel()

	select {
	case <-consumeDone:
	case <-time.After(200 * time.Millisecond):
		t.Error("workers did not stop after context cancellation")
	}
}
