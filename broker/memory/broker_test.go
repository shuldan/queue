package memory

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/shuldan/queue"
)

func TestBroker_ProduceConsume(t *testing.T) {
	b := New()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	received := make(chan []byte, 1)
	consumeDone := make(chan error, 1)

	go func() {
		consumeDone <- b.Consume(ctx, "test", func(data []byte) error {
			received <- data
			return nil
		})
	}()

	time.Sleep(10 * time.Millisecond)

	err := b.Produce(context.Background(), "test", []byte("hello"))
	if err != nil {
		t.Fatalf("Produce failed: %v", err)
	}

	select {
	case data := <-received:
		if string(data) != "hello" {
			t.Errorf("expected 'hello', got %q", string(data))
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("timeout waiting for message")
	}

	cancel()

	select {
	case err := <-consumeDone:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("expected context.Canceled, got %v", err)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Consume did not finish")
	}
}

func TestBroker_Consume_BlocksUntilContextDone(t *testing.T) {
	t.Parallel()

	b := New()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	consumeStarted := make(chan struct{})
	consumeDone := make(chan error, 1)

	go func() {
		close(consumeStarted)
		consumeDone <- b.Consume(ctx, "test", func(data []byte) error {
			return nil
		})
	}()

	<-consumeStarted

	select {
	case <-consumeDone:
		t.Error("Consume should not finish before context is cancelled")
	case <-time.After(20 * time.Millisecond):
	}

	cancel()

	select {
	case err := <-consumeDone:
		if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
			t.Errorf("expected context error, got %v", err)
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("Consume did not finish after context cancellation")
	}
}

func TestBroker_ConcurrentConsume(t *testing.T) {
	b := New()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var count atomic.Int32
	expected := 5

	for i := 0; i < 3; i++ {
		go func() {
			_ = b.Consume(ctx, "shared", func([]byte) error {
				count.Add(1)
				return nil
			})
		}()
	}

	time.Sleep(20 * time.Millisecond)

	for i := 0; i < expected; i++ {
		_ = b.Produce(context.Background(), "shared", []byte("msg"))
	}

	time.Sleep(50 * time.Millisecond)

	actualCount := count.Load()
	if actualCount != int32(expected) {
		t.Errorf("expected %d messages, got %d", expected, actualCount)
	}

	cancel()
	time.Sleep(20 * time.Millisecond)
}

func TestBroker_Produce_BrokerClosed(t *testing.T) {
	t.Parallel()

	b := New()
	err := b.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	err = b.Produce(context.Background(), "test", []byte("after"))
	if !errors.Is(err, queue.ErrQueueClosed) {
		t.Errorf("expected ErrQueueClosed, got %v", err)
	}
}

func TestBroker_Consume_BrokerClosed(t *testing.T) {
	t.Parallel()

	b := New()
	err := b.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	err = b.Consume(context.Background(), "test", func([]byte) error { return nil })
	if !errors.Is(err, queue.ErrQueueClosed) {
		t.Errorf("expected ErrQueueClosed, got %v", err)
	}
}

func TestBroker_Close_WaitsForConsumers(t *testing.T) {
	b := New()
	ctx, cancel := context.WithCancel(context.Background())

	consumeStarted := make(chan struct{})
	go func() {
		close(consumeStarted)
		_ = b.Consume(ctx, "test", func([]byte) error {
			time.Sleep(50 * time.Millisecond)
			return nil
		})
	}()

	<-consumeStarted
	time.Sleep(10 * time.Millisecond)

	_ = b.Produce(context.Background(), "test", []byte("msg"))

	cancel()

	closeDone := make(chan struct{})
	go func() {
		_ = b.Close()
		close(closeDone)
	}()

	select {
	case <-closeDone:
	case <-time.After(200 * time.Millisecond):
		t.Error("Close did not wait for consumers to finish")
	}
}

func TestBroker_Close_AlreadyClosed(t *testing.T) {
	t.Parallel()

	b := New()
	err := b.Close()
	if err != nil {
		t.Fatalf("first Close failed: %v", err)
	}

	err = b.Close()
	if err != nil {
		t.Errorf("second Close should not error, got %v", err)
	}
}

func TestBroker_Produce_ContextCanceled(t *testing.T) {
	t.Parallel()

	b := New()

	for i := 0; i < 100; i++ {
		err := b.Produce(context.Background(), "test", []byte("fill"))
		if err != nil {
			t.Fatalf("failed to fill buffer: %v", err)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := b.Produce(ctx, "test", []byte("should fail"))
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled, got %v", err)
	}
}

func TestBroker_HandlerPanic(t *testing.T) {
	b := New()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	panicHandled := make(chan struct{})
	messageReceived := make(chan struct{})

	go func() {
		_ = b.Consume(ctx, "panic-test", func(data []byte) error {
			close(messageReceived)
			defer func() {
				if r := recover(); r != nil {
					close(panicHandled)
				}
			}()
			panic("test panic")
		})
	}()

	time.Sleep(10 * time.Millisecond)

	err := b.Produce(context.Background(), "panic-test", []byte("trigger"))
	if err != nil {
		t.Fatalf("Produce failed: %v", err)
	}

	select {
	case <-messageReceived:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("message was not received")
	}

	time.Sleep(20 * time.Millisecond)

	cancel()
}

func TestBroker_MultipleTopic(t *testing.T) {
	t.Parallel()

	b := New()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	received1 := make(chan []byte, 1)
	received2 := make(chan []byte, 1)

	go func() {
		_ = b.Consume(ctx, "topic1", func(data []byte) error {
			received1 <- data
			return nil
		})
	}()

	go func() {
		_ = b.Consume(ctx, "topic2", func(data []byte) error {
			received2 <- data
			return nil
		})
	}()

	time.Sleep(10 * time.Millisecond)

	_ = b.Produce(context.Background(), "topic1", []byte("msg1"))
	_ = b.Produce(context.Background(), "topic2", []byte("msg2"))

	var r1, r2 string

	select {
	case data := <-received1:
		r1 = string(data)
	case <-time.After(100 * time.Millisecond):
		t.Fatal("timeout waiting for topic1 message")
	}

	select {
	case data := <-received2:
		r2 = string(data)
	case <-time.After(100 * time.Millisecond):
		t.Fatal("timeout waiting for topic2 message")
	}

	if r1 != "msg1" {
		t.Errorf("expected 'msg1' on topic1, got %q", r1)
	}
	if r2 != "msg2" {
		t.Errorf("expected 'msg2' on topic2, got %q", r2)
	}

	cancel()
}

func TestBroker_HandlerError(t *testing.T) {
	t.Parallel()

	b := New()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	handlerCalled := make(chan struct{})

	go func() {
		_ = b.Consume(ctx, "test", func(data []byte) error {
			close(handlerCalled)
			return errors.New("handler error")
		})
	}()

	time.Sleep(10 * time.Millisecond)

	err := b.Produce(context.Background(), "test", []byte("msg"))
	if err != nil {
		t.Fatalf("Produce failed: %v", err)
	}

	select {
	case <-handlerCalled:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("handler was not called")
	}

	cancel()
}

func TestBroker_MultipleMessages(t *testing.T) {
	t.Parallel()

	b := New()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const numMessages = 10
	received := make(chan []byte, numMessages)

	go func() {
		_ = b.Consume(ctx, "test", func(data []byte) error {
			received <- data
			return nil
		})
	}()

	time.Sleep(10 * time.Millisecond)

	for i := 0; i < numMessages; i++ {
		err := b.Produce(context.Background(), "test", []byte("msg"))
		if err != nil {
			t.Fatalf("Produce failed: %v", err)
		}
	}

	count := 0
	timeout := time.After(200 * time.Millisecond)
	for count < numMessages {
		select {
		case <-received:
			count++
		case <-timeout:
			t.Fatalf("timeout: received %d/%d messages", count, numMessages)
		}
	}

	cancel()
}

func TestBroker_ConsumeStopsOnContextCancel(t *testing.T) {
	t.Parallel()

	b := New()
	ctx, cancel := context.WithCancel(context.Background())

	consumeDone := make(chan error, 1)
	messageReceived := make(chan struct{})

	go func() {
		consumeDone <- b.Consume(ctx, "test", func(data []byte) error {
			close(messageReceived)
			return nil
		})
	}()

	time.Sleep(10 * time.Millisecond)

	_ = b.Produce(context.Background(), "test", []byte("msg"))

	select {
	case <-messageReceived:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("message not received")
	}

	cancel()

	select {
	case err := <-consumeDone:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("expected context.Canceled, got %v", err)
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("Consume did not stop after context cancellation")
	}
}
