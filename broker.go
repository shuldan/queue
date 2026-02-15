package queue

import "context"

type Broker interface {
	Produce(ctx context.Context, topic string, data []byte) error
	Consume(ctx context.Context, topic string, handler func([]byte) error) error
	Ping(ctx context.Context) error
	Close() error
}
