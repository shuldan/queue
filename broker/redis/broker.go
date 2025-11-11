package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"

	"github.com/shuldan/queue"
)

type broker struct {
	client      redis.UniversalClient
	consumers   map[string][]context.CancelFunc
	consumersMu sync.RWMutex
	opts        *options
	wg          sync.WaitGroup
	closed      bool
	closedMu    sync.RWMutex
}

func New(client redis.UniversalClient, opts ...Option) queue.Broker {
	b := &broker{
		client:    client,
		consumers: make(map[string][]context.CancelFunc),
		opts: &options{
			streamKeyFormat:   "stream:%s",
			consumerGroup:     "consumers",
			processingTimeout: 30 * time.Second,
			claimInterval:     1 * time.Second,
			maxClaimBatch:     10,
			blockTimeout:      500 * time.Millisecond,
			maxStreamLength:   0,
			approximateTrim:   true,
			enableClaim:       true,
			consumerPrefix:    "",
		},
	}

	for _, opt := range opts {
		opt(b.opts)
	}

	return b
}

func (b *broker) Produce(ctx context.Context, topic string, data []byte) error {
	b.closedMu.RLock()
	if b.closed {
		b.closedMu.RUnlock()
		return ErrBrokerClosed
	}
	b.closedMu.RUnlock()

	msg := redisStreamMessage{
		Data:       data,
		EnqueuedAt: time.Now().UTC().Format(time.RFC3339),
	}

	values, err := b.encodeMessage(msg)
	if err != nil {
		return fmt.Errorf("%w: topic=%s, err=%v", ErrEncodeFailed, topic, err)
	}

	stream := fmt.Sprintf(b.opts.streamKeyFormat, topic)

	xAddArgs := &redis.XAddArgs{
		Stream: stream,
		Values: values,
	}

	if b.opts.maxStreamLength > 0 {
		xAddArgs.MaxLen = b.opts.maxStreamLength
		xAddArgs.Approx = b.opts.approximateTrim
	}

	if _, err = b.client.XAdd(ctx, xAddArgs).Result(); err != nil {
		return fmt.Errorf("%w: topic=%s, stream=%s, err=%v", ErrProduceFailed, topic, stream, err)
	}

	return nil
}

func (b *broker) Consume(ctx context.Context, topic string, handler func([]byte) error) error {
	b.closedMu.RLock()
	if b.closed {
		b.closedMu.RUnlock()
		return ErrBrokerClosed
	}
	b.closedMu.RUnlock()

	stream := fmt.Sprintf(b.opts.streamKeyFormat, topic)
	group := fmt.Sprintf("%s:%s", b.opts.consumerGroup, topic)
	consumer := b.newConsumerID(topic)

	exists, err := b.groupExists(ctx, stream, group)
	if err != nil {
		return fmt.Errorf("%w: topic=%s, stream=%s, group=%s, err=%v",
			ErrGroupCheckFailed, topic, stream, group, err)
	}

	if !exists {
		if err := b.client.XGroupCreateMkStream(ctx, stream, group, "0").Err(); err != nil {
			if !isGroupExists(err) {
				return fmt.Errorf("%w: topic=%s, stream=%s, group=%s, err=%v",
					ErrConsumeSetupFailed, topic, stream, group, err)
			}
		}
	}

	consumerCtx, cancel := context.WithCancel(ctx)
	b.trackConsumer(topic, cancel)

	b.wg.Add(1)
	go func() {
		defer b.wg.Done()
		b.consumeLoop(consumerCtx, stream, group, consumer, handler)
	}()

	<-ctx.Done()
	cancel()

	return ctx.Err()
}

func (b *broker) consumeLoop(
	ctx context.Context,
	stream, group, consumer string,
	handler func([]byte) error,
) {
	ticker := time.NewTicker(b.opts.claimInterval)
	if !b.opts.enableClaim {
		ticker.Stop()
	}
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if b.opts.enableClaim {
				b.claimStalledMessages(ctx, stream, group, consumer, handler)
			}
		default:
			b.processNewMessage(ctx, stream, group, consumer, handler)
		}
	}
}

func (b *broker) processNewMessage(
	ctx context.Context,
	stream, group, consumer string,
	handler func([]byte) error,
) {
	result, err := b.client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    group,
		Consumer: consumer,
		Streams:  []string{stream, ">"},
		Count:    1,
		Block:    b.opts.blockTimeout,
		NoAck:    false,
	}).Result()

	if err != nil {
		if errors.Is(err, redis.Nil) || ctx.Err() != nil {
			select {
			case <-time.After(10 * time.Millisecond):
			case <-ctx.Done():
			}
			return
		}
		return
	}

	if len(result) == 0 || len(result[0].Messages) == 0 {
		return
	}

	msg := result[0].Messages[0]
	var body redisStreamMessage
	if err := b.decodeMessage(msg.Values, &body); err != nil {
		_ = b.client.XAck(ctx, stream, group, msg.ID)
		return
	}

	err = handler(body.Data)
	if err == nil {
		_ = b.client.XAck(ctx, stream, group, msg.ID)
	}
}

func (b *broker) claimStalledMessages(
	ctx context.Context,
	stream, group, consumer string,
	handler func([]byte) error,
) {
	ids, err := b.client.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream:   stream,
		Group:    group,
		Start:    "-",
		End:      "+",
		Count:    int64(b.opts.maxClaimBatch),
		Idle:     b.opts.processingTimeout,
		Consumer: "",
	}).Result()
	if err != nil {
		return
	}

	for _, id := range ids {
		msgs, _ := b.client.XClaim(ctx, &redis.XClaimArgs{
			Stream:   stream,
			Group:    group,
			Consumer: consumer,
			MinIdle:  b.opts.processingTimeout,
			Messages: []string{id.ID},
		}).Result()

		for _, msg := range msgs {
			var body redisStreamMessage
			if err := b.decodeMessage(msg.Values, &body); err != nil {
				_ = b.client.XAck(ctx, stream, group, msg.ID)
				continue
			}

			err := handler(body.Data)
			if err == nil {
				_ = b.client.XAck(ctx, stream, group, msg.ID)
			}
		}
	}
}

func (b *broker) Close() error {
	b.closedMu.Lock()
	if b.closed {
		b.closedMu.Unlock()
		return nil
	}
	b.closed = true
	b.closedMu.Unlock()

	b.consumersMu.Lock()
	for topic, cancels := range b.consumers {
		for _, cancel := range cancels {
			if cancel != nil {
				cancel()
			}
		}
		b.consumers[topic] = nil
	}
	b.consumersMu.Unlock()

	b.wg.Wait()
	return nil
}

func (b *broker) groupExists(ctx context.Context, stream, group string) (bool, error) {
	groups, err := b.client.XInfoGroups(ctx, stream).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return false, nil
		}
		return false, err
	}

	for _, g := range groups {
		if g.Name == group {
			return true, nil
		}
	}

	return false, nil
}

func (b *broker) trackConsumer(topic string, cancel context.CancelFunc) {
	b.consumersMu.Lock()
	b.consumers[topic] = append(b.consumers[topic], cancel)
	b.consumersMu.Unlock()
}

func (b *broker) newConsumerID(topic string) string {
	prefix := b.opts.consumerPrefix
	if prefix != "" {
		prefix += "-"
	}
	return fmt.Sprintf("consumer-%s%s-%s", prefix, topic, uuid.New().String())
}

func (b *broker) encodeMessage(msg redisStreamMessage) (map[string]interface{}, error) {
	data, err := json.Marshal(msg)
	if err != nil {
		return nil, err
	}
	return map[string]interface{}{"payload": string(data)}, nil
}

func (b *broker) decodeMessage(values map[string]interface{}, msg *redisStreamMessage) error {
	payload, ok := values["payload"].(string)
	if !ok {
		return ErrInvalidPayload
	}
	return json.Unmarshal([]byte(payload), msg)
}

func isGroupExists(err error) bool {
	return err != nil && (strings.HasPrefix(err.Error(), "BUSYGROUP") ||
		strings.Contains(err.Error(), "already exists"))
}

type redisStreamMessage struct {
	Data       []byte `json:"data"`
	EnqueuedAt string `json:"enqueued_at"`
}
