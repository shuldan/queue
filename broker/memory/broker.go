package memory

import (
	"context"
	"log/slog"
	"runtime/debug"
	"sync"

	"github.com/shuldan/queue"
)

type broker struct {
	mu       sync.RWMutex
	channels map[string]chan []byte
	done     chan struct{}
	wg       sync.WaitGroup
	closed   bool
}

func New() queue.Broker {
	return &broker{
		channels: make(map[string]chan []byte),
		done:     make(chan struct{}),
	}
}

func (b *broker) getOrCreateChan(topic string) chan []byte {
	b.mu.Lock()
	defer b.mu.Unlock()

	if ch, ok := b.channels[topic]; ok {
		return ch
	}

	ch := make(chan []byte, 100)
	b.channels[topic] = ch

	return ch
}

func (b *broker) Produce(
	ctx context.Context, topic string, data []byte,
) error {
	if err := b.checkClosed(); err != nil {
		return err
	}

	ch := b.getOrCreateChan(topic)

	select {
	case ch <- data:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (b *broker) Consume(
	ctx context.Context,
	topic string,
	handler func([]byte) error,
) error {
	if err := b.checkClosed(); err != nil {
		return err
	}

	ch := b.getOrCreateChan(topic)

	b.wg.Add(1)

	go func() {
		defer b.wg.Done()
		b.consumeLoop(ctx, ch, topic, handler)
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-b.done:
		return queue.ErrBrokerClosed
	}
}

func (b *broker) consumeLoop(
	ctx context.Context,
	ch <-chan []byte,
	topic string,
	handler func([]byte) error,
) {
	defer func() {
		if r := recover(); r != nil {
			b.logPanic(topic, r)
		}
	}()

	for {
		select {
		case data, ok := <-ch:
			if !ok {
				return
			}

			b.safeHandle(ctx, data, topic, handler)
		case <-ctx.Done():
			b.drainChannel(ch, topic, handler)

			return
		case <-b.done:
			b.drainChannel(ch, topic, handler)

			return
		}
	}
}

func (b *broker) drainChannel(
	ch <-chan []byte,
	topic string,
	handler func([]byte) error,
) {
	for {
		select {
		case data, ok := <-ch:
			if !ok {
				return
			}

			b.safeHandle(context.Background(), data, topic, handler)
		default:
			return
		}
	}
}

func (b *broker) safeHandle(
	ctx context.Context,
	data []byte,
	topic string,
	handler func([]byte) error,
) {
	defer func() {
		if r := recover(); r != nil {
			b.logPanic(topic, r)
		}
	}()

	select {
	case <-ctx.Done():
		return
	default:
	}

	_ = handler(data)
}

func (b *broker) logPanic(topic string, r any) {
	slog.Error("panic in message handler",
		"topic", topic,
		"panic", r,
		"stack", string(debug.Stack()),
	)
}

func (b *broker) Ping(_ context.Context) error {
	return b.checkClosed()
}

func (b *broker) Close() error {
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

func (b *broker) checkClosed() error {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if b.closed {
		return queue.ErrBrokerClosed
	}

	return nil
}
