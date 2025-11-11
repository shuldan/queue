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
	wg       sync.WaitGroup
	closed   bool
}

func New() queue.Broker {
	return &broker{
		channels: make(map[string]chan []byte),
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

func (b *broker) Produce(ctx context.Context, topic string, data []byte) error {
	b.mu.RLock()
	if b.closed {
		b.mu.RUnlock()
		return queue.ErrQueueClosed
	}
	b.mu.RUnlock()

	ch := b.getOrCreateChan(topic)
	select {
	case ch <- data:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (b *broker) Consume(ctx context.Context, topic string, handler func([]byte) error) error {
	b.mu.RLock()
	if b.closed {
		b.mu.RUnlock()
		return queue.ErrQueueClosed
	}
	b.mu.RUnlock()

	ch := b.getOrCreateChan(topic)

	b.wg.Add(1)
	go func() {
		defer b.wg.Done()
		b.consumeMessages(ctx, ch, topic, handler)
	}()

	<-ctx.Done()
	return ctx.Err()
}

func (b *broker) consumeMessages(ctx context.Context, ch chan []byte, topic string, handler func([]byte) error) {
	defer func() {
		if r := recover(); r != nil {
			b.handlePanic(topic, r)
		}
	}()

	for {
		select {
		case data, ok := <-ch:
			if !ok {
				return
			}
			b.handleMessage(ctx, data, topic, handler)
		case <-ctx.Done():
			return
		}
	}
}

func (b *broker) handleMessage(ctx context.Context, data []byte, topic string, handler func([]byte) error) {
	defer func() {
		if r := recover(); r != nil {
			b.handlePanic(topic, r)
		}
	}()

	select {
	case <-ctx.Done():
		return
	default:
	}

	_ = handler(data)
}

func (b *broker) handlePanic(topic string, r interface{}) {
	slog.Error(
		"panic in message handler",
		"topic", topic,
		"panic", r,
		"stack", string(debug.Stack()),
	)
}

func (b *broker) Close() error {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil
	}
	b.closed = true

	for topic := range b.channels {
		close(b.channels[topic])
		delete(b.channels, topic)
	}
	b.mu.Unlock()

	b.wg.Wait()
	return nil
}
