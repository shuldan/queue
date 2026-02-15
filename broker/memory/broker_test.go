package memory

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/shuldan/queue"
)

func newTestBroker() *broker {
	return New().(*broker)
}

func produceAndWaitHandler(
	t *testing.T, b *broker, topic string, ready chan struct{},
) {
	t.Helper()
	deadline := time.After(2 * time.Second)
	for {
		err := b.Produce(context.Background(), topic, []byte("ping"))
		if err == nil {
			break
		}
		select {
		case <-deadline:
			t.Fatal("timed out producing warmup message")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	select {
	case <-ready:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for handler to be called")
	}
}

func TestNew_CreatesBroker(t *testing.T) {
	t.Parallel()
	b := New()
	if b == nil {
		t.Fatal("expected non-nil broker")
	}
}

func TestProduce_BasicMessage(t *testing.T) {
	t.Parallel()
	b := newTestBroker()
	defer b.Close()
	err := b.Produce(context.Background(), "topic1", []byte("hello"))
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
}

func TestProduce_ClosedBroker(t *testing.T) {
	t.Parallel()
	b := newTestBroker()
	b.Close()
	err := b.Produce(context.Background(), "topic1", []byte("hello"))
	if !errors.Is(err, queue.ErrBrokerClosed) {
		t.Errorf("expected ErrBrokerClosed, got %v", err)
	}
}

func TestProduce_CancelledContext(t *testing.T) {
	t.Parallel()
	b := newTestBroker()
	defer b.Close()
	ch := b.getOrCreateChan("full")
	for range 100 {
		ch <- []byte("x")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := b.Produce(ctx, "full", []byte("overflow"))
	if err == nil {
		t.Fatal("expected context error")
	}
}

func TestConsume_BasicFlow(t *testing.T) {
	t.Parallel()
	b := newTestBroker()

	var received atomic.Value
	ready := make(chan struct{}, 10)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	doneCh := make(chan error, 1)
	go func() {
		doneCh <- b.Consume(ctx, "t", func(data []byte) error {
			received.Store(string(data))
			select {
			case ready <- struct{}{}:
			default:
			}
			return nil
		})
	}()

	produceAndWaitHandler(t, b, "t", ready)

	if err := b.Produce(context.Background(), "t", []byte("real")); err != nil {
		t.Fatalf("produce error: %v", err)
	}
	select {
	case <-ready:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for second message")
	}

	cancel()
	<-doneCh

	v, _ := received.Load().(string)
	if v != "real" {
		t.Errorf("expected 'real', got %q", v)
	}
}

func TestConsume_ClosedBroker(t *testing.T) {
	t.Parallel()
	b := newTestBroker()
	b.Close()
	err := b.Consume(context.Background(), "t", func([]byte) error { return nil })
	if !errors.Is(err, queue.ErrBrokerClosed) {
		t.Errorf("expected ErrBrokerClosed, got %v", err)
	}
}

func TestConsume_BrokerCloseWhileConsuming(t *testing.T) {
	t.Parallel()
	b := newTestBroker()

	ready := make(chan struct{}, 1)
	errCh := make(chan error, 1)
	go func() {
		errCh <- b.Consume(context.Background(), "t", func([]byte) error {
			select {
			case ready <- struct{}{}:
			default:
			}
			return nil
		})
	}()

	produceAndWaitHandler(t, b, "t", ready)

	b.Close()
	err := <-errCh
	if !errors.Is(err, queue.ErrBrokerClosed) {
		t.Errorf("expected ErrBrokerClosed, got %v", err)
	}
}

func TestConsume_HandlerPanic(t *testing.T) {
	t.Parallel()
	b := newTestBroker()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var callCount atomic.Int32
	ready := make(chan struct{}, 10)
	doneCh := make(chan error, 1)
	go func() {
		doneCh <- b.Consume(ctx, "panic-topic", func([]byte) error {
			n := callCount.Add(1)
			select {
			case ready <- struct{}{}:
			default:
			}
			if n >= 2 {
				panic("test panic")
			}
			return nil
		})
	}()

	produceAndWaitHandler(t, b, "panic-topic", ready)

	_ = b.Produce(context.Background(), "panic-topic", []byte("boom"))
	select {
	case <-ready:
	case <-time.After(2 * time.Second):
	}
	time.Sleep(50 * time.Millisecond)

	cancel()
	<-doneCh
}

func TestPing_OpenBroker(t *testing.T) {
	t.Parallel()
	b := newTestBroker()
	defer b.Close()
	if err := b.Ping(context.Background()); err != nil {
		t.Errorf("expected nil, got %v", err)
	}
}

func TestPing_ClosedBroker(t *testing.T) {
	t.Parallel()
	b := newTestBroker()
	b.Close()
	err := b.Ping(context.Background())
	if !errors.Is(err, queue.ErrBrokerClosed) {
		t.Errorf("expected ErrBrokerClosed, got %v", err)
	}
}

func TestClose_Idempotent(t *testing.T) {
	t.Parallel()
	b := newTestBroker()
	if err := b.Close(); err != nil {
		t.Errorf("first close: %v", err)
	}
	if err := b.Close(); err != nil {
		t.Errorf("second close: %v", err)
	}
}

func TestGetOrCreateChan_ReturnsSameChannel(t *testing.T) {
	t.Parallel()
	b := newTestBroker()
	defer b.Close()
	ch1 := b.getOrCreateChan("topic")
	ch2 := b.getOrCreateChan("topic")
	if ch1 != ch2 {
		t.Error("expected same channel for same topic")
	}
}

func TestDrainChannel_EmptyChannel(t *testing.T) {
	t.Parallel()
	b := newTestBroker()
	defer b.Close()
	ch := make(chan []byte, 5)
	called := false
	b.drainChannel(ch, "t", func([]byte) error {
		called = true
		return nil
	})
	if called {
		t.Error("handler should not be called on empty channel")
	}
}

func TestDrainChannel_WithData(t *testing.T) {
	t.Parallel()
	b := newTestBroker()
	defer b.Close()
	ch := make(chan []byte, 5)
	ch <- []byte("msg1")
	ch <- []byte("msg2")
	var count int
	b.drainChannel(ch, "t", func([]byte) error {
		count++
		return nil
	})
	if count != 2 {
		t.Errorf("expected 2 calls, got %d", count)
	}
}

func TestDrainChannel_ClosedChannel(t *testing.T) {
	t.Parallel()
	b := newTestBroker()
	defer b.Close()
	ch := make(chan []byte, 5)
	ch <- []byte("msg")
	close(ch)
	var count int
	b.drainChannel(ch, "t", func([]byte) error {
		count++
		return nil
	})
	if count != 1 {
		t.Errorf("expected 1 call, got %d", count)
	}
}

func TestSafeHandle_CancelledContext(t *testing.T) {
	t.Parallel()
	b := newTestBroker()
	defer b.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	called := false
	b.safeHandle(ctx, []byte("x"), "t", func([]byte) error {
		called = true
		return nil
	})
	if called {
		t.Error("handler should not be called when context is cancelled")
	}
}

func TestSafeHandle_PanicRecovery(t *testing.T) {
	t.Parallel()
	b := newTestBroker()
	defer b.Close()
	b.safeHandle(context.Background(), []byte("x"), "t", func([]byte) error {
		panic("boom")
	})
}

func TestConsume_MultipleTopics(t *testing.T) {
	t.Parallel()
	b := newTestBroker()

	var count1, count2 atomic.Int32
	ready1 := make(chan struct{}, 10)
	ready2 := make(chan struct{}, 10)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		_ = b.Consume(ctx, "t1", func([]byte) error {
			count1.Add(1)
			select {
			case ready1 <- struct{}{}:
			default:
			}
			return nil
		})
	}()
	go func() {
		defer wg.Done()
		_ = b.Consume(ctx, "t2", func([]byte) error {
			count2.Add(1)
			select {
			case ready2 <- struct{}{}:
			default:
			}
			return nil
		})
	}()

	produceAndWaitHandler(t, b, "t1", ready1)
	produceAndWaitHandler(t, b, "t2", ready2)

	cancel()
	wg.Wait()

	if count1.Load() < 1 {
		t.Errorf("t1: expected >= 1, got %d", count1.Load())
	}
	if count2.Load() < 1 {
		t.Errorf("t2: expected >= 1, got %d", count2.Load())
	}
}
