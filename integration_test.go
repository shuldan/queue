package queue

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type inMemoryBroker struct {
	mu       sync.RWMutex
	topics   map[string][][]byte
	handlers map[string]func([]byte) error
	closed   bool
}

func newInMemoryBroker() *inMemoryBroker {
	return &inMemoryBroker{
		topics:   make(map[string][][]byte),
		handlers: make(map[string]func([]byte) error),
	}
}

func (b *inMemoryBroker) Produce(ctx context.Context, topic string, data []byte) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return ErrQueueClosed
	}

	dataCopy := make([]byte, len(data))
	copy(dataCopy, data)
	b.topics[topic] = append(b.topics[topic], dataCopy)

	if handler, ok := b.handlers[topic]; ok {
		go handler(dataCopy)
	}

	return nil
}

func (b *inMemoryBroker) Consume(ctx context.Context, topic string, handler func([]byte) error) error {
	b.mu.Lock()
	b.handlers[topic] = handler

	existing := b.topics[topic]
	b.mu.Unlock()

	for _, data := range existing {
		if err := handler(data); err != nil {
			return err
		}
	}

	<-ctx.Done()
	return ctx.Err()
}

func (b *inMemoryBroker) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.closed = true
	return nil
}

func TestQueue_FullIntegration_ProduceAndConsume(t *testing.T) {
	broker := newInMemoryBroker()
	q, err := New[*TestJob](broker, WithWorkerCount(2))
	if err != nil {
		t.Fatalf("failed to create queue: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	var processed int32

	go func() {
		_ = q.Consume(ctx, func(ctx context.Context, job *TestJob) error {
			atomic.AddInt32(&processed, 1)
			return nil
		})
	}()

	time.Sleep(10 * time.Millisecond)

	for i := 0; i < 10; i++ {
		err := q.Produce(context.Background(), &TestJob{ID: i, Name: "job"})
		if err != nil {
			t.Errorf("failed to produce job %d: %v", i, err)
		}
	}

	time.Sleep(100 * time.Millisecond)

	count := atomic.LoadInt32(&processed)
	if count != 10 {
		t.Errorf("expected 10 jobs processed, got %d", count)
	}

	cancel()
}

func TestQueue_FullIntegration_CloseWhileProducing(t *testing.T) {
	broker := newInMemoryBroker()
	q, _ := New[*TestJob](broker)

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		time.Sleep(10 * time.Millisecond)
		_ = q.Close()
	}()

	time.Sleep(5 * time.Millisecond)

	for i := 0; i < 100; i++ {
		_ = q.Produce(context.Background(), &TestJob{ID: i})
		time.Sleep(time.Microsecond)
	}

	wg.Wait()
}
