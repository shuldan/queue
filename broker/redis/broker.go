package redis

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"

	"github.com/shuldan/queue"
)

const (
	payloadField   = "payload"
	cleanupTimeout = 5 * time.Second
	errorBackoff   = time.Second
	defaultStartID = "0"
	pendingStartID = "-"
	pendingEndID   = "+"
	newMessageID   = ">"
)

type broker struct {
	client   redis.UniversalClient
	opts     *options
	done     chan struct{}
	wg       sync.WaitGroup
	closed   bool
	closedMu sync.RWMutex
}

func New(client redis.UniversalClient, opts ...Option) queue.Broker {
	b := &broker{
		client: client,
		done:   make(chan struct{}),
		opts: &options{
			streamKeyFormat:   "stream:%s",
			consumerGroup:     "consumers",
			processingTimeout: 30 * time.Second,
			claimInterval:     time.Second,
			maxClaimBatch:     10,
			blockTimeout:      500 * time.Millisecond,
			approximateTrim:   true,
			enableClaim:       true,
		},
	}

	for _, opt := range opts {
		opt(b.opts)
	}

	return b
}

func (b *broker) Produce(
	ctx context.Context, topic string, data []byte,
) error {
	if err := b.checkClosed(); err != nil {
		return err
	}

	stream := b.streamKey(topic)
	args := b.buildXAddArgs(stream, data)

	_, err := b.client.XAdd(ctx, args).Result()
	if err != nil {
		return fmt.Errorf("%w: stream=%s: %w",
			ErrProduceFailed, stream, err)
	}

	return nil
}

func (b *broker) buildXAddArgs(
	stream string, data []byte,
) *redis.XAddArgs {
	args := &redis.XAddArgs{
		Stream: stream,
		Values: map[string]interface{}{payloadField: string(data)},
	}

	if b.opts.maxStreamLength > 0 {
		args.MaxLen = b.opts.maxStreamLength
		args.Approx = b.opts.approximateTrim
	}

	return args
}

func (b *broker) Consume(
	ctx context.Context,
	topic string,
	handler func([]byte) error,
) error {
	if err := b.checkClosed(); err != nil {
		return err
	}

	stream, group, consumer := b.consumerNames(topic)

	if err := b.ensureGroup(ctx, stream, group); err != nil {
		return err
	}

	return b.startConsumer(ctx, stream, group, consumer, handler)
}

func (b *broker) consumerNames(
	topic string,
) (string, string, string) {
	stream := b.streamKey(topic)
	group := fmt.Sprintf("%s:%s", b.opts.consumerGroup, topic)
	consumer := b.newConsumerID(topic)

	return stream, group, consumer
}

func (b *broker) ensureGroup(
	ctx context.Context, stream, group string,
) error {
	exists, err := b.groupExists(ctx, stream, group)
	if err != nil {
		return fmt.Errorf("%w: stream=%s, group=%s: %w",
			ErrGroupCheckFailed, stream, group, err)
	}

	if exists {
		return nil
	}

	err = b.client.XGroupCreateMkStream(
		ctx, stream, group, defaultStartID,
	).Err()
	if err != nil && !isGroupExistsErr(err) {
		return fmt.Errorf("%w: stream=%s, group=%s: %w",
			ErrConsumeSetupFailed, stream, group, err)
	}

	return nil
}

func (b *broker) startConsumer(
	ctx context.Context,
	stream, group, consumer string,
	handler func([]byte) error,
) error {
	consumerCtx, cancel := context.WithCancel(ctx)

	b.wg.Add(1)

	go func() {
		defer b.wg.Done()
		defer cancel()

		b.consumeLoop(consumerCtx, stream, group, consumer, handler)
	}()

	select {
	case <-ctx.Done():
		cancel()

		return ctx.Err()
	case <-b.done:
		cancel()

		return queue.ErrBrokerClosed
	}
}

func (b *broker) consumeLoop(
	ctx context.Context,
	stream, group, consumer string,
	handler func([]byte) error,
) {
	defer b.cleanupConsumer(stream, group, consumer)

	if b.opts.enableClaim {
		b.wg.Add(1)

		go func() {
			defer b.wg.Done()
			b.claimLoop(ctx, stream, group, consumer, handler)
		}()
	}

	for {
		select {
		case <-ctx.Done():
			return
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
		Streams:  []string{stream, newMessageID},
		Count:    1,
		Block:    b.opts.blockTimeout,
		NoAck:    false,
	}).Result()

	if err != nil {
		b.handleReadError(ctx, err)

		return
	}

	if len(result) == 0 || len(result[0].Messages) == 0 {
		return
	}

	b.handleStreamMessage(ctx, stream, group, result[0].Messages[0], handler)
}

func (b *broker) handleReadError(ctx context.Context, err error) {
	if errors.Is(err, redis.Nil) || ctx.Err() != nil {
		return
	}

	timer := time.NewTimer(errorBackoff)
	defer timer.Stop()

	select {
	case <-timer.C:
	case <-ctx.Done():
	}
}

func (b *broker) handleStreamMessage(
	ctx context.Context,
	stream, group string,
	msg redis.XMessage,
	handler func([]byte) error,
) {
	payload, err := extractPayload(msg.Values)
	if err != nil {
		_ = b.client.XAck(ctx, stream, group, msg.ID).Err()

		return
	}

	err = handler(payload)
	if err == nil {
		_ = b.client.XAck(ctx, stream, group, msg.ID).Err()
	}
}

func (b *broker) claimLoop(
	ctx context.Context,
	stream, group, consumer string,
	handler func([]byte) error,
) {
	ticker := time.NewTicker(b.opts.claimInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			b.claimStalledMessages(
				ctx, stream, group, consumer, handler,
			)
		}
	}
}

func (b *broker) claimStalledMessages(
	ctx context.Context,
	stream, group, consumer string,
	handler func([]byte) error,
) {
	pending := b.findPendingMessages(ctx, stream, group)
	for _, p := range pending {
		b.claimAndHandle(ctx, stream, group, consumer, p.ID, handler)
	}
}

func (b *broker) findPendingMessages(
	ctx context.Context,
	stream, group string,
) []redis.XPendingExt {
	msgs, err := b.client.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream: stream,
		Group:  group,
		Start:  pendingStartID,
		End:    pendingEndID,
		Count:  int64(b.opts.maxClaimBatch),
		Idle:   b.opts.processingTimeout,
	}).Result()
	if err != nil {
		return nil
	}

	return msgs
}

func (b *broker) claimAndHandle(
	ctx context.Context,
	stream, group, consumer, msgID string,
	handler func([]byte) error,
) {
	msgs, err := b.client.XClaim(ctx, &redis.XClaimArgs{
		Stream:   stream,
		Group:    group,
		Consumer: consumer,
		MinIdle:  b.opts.processingTimeout,
		Messages: []string{msgID},
	}).Result()
	if err != nil {
		return
	}

	for i := range msgs {
		b.handleStreamMessage(ctx, stream, group, msgs[i], handler)
	}
}

func (b *broker) Ping(ctx context.Context) error {
	return b.client.Ping(ctx).Err()
}

func (b *broker) Close() error {
	b.closedMu.Lock()
	if b.closed {
		b.closedMu.Unlock()

		return nil
	}

	b.closed = true
	close(b.done)
	b.closedMu.Unlock()

	b.wg.Wait()

	return nil
}

func (b *broker) checkClosed() error {
	b.closedMu.RLock()
	defer b.closedMu.RUnlock()

	if b.closed {
		return queue.ErrBrokerClosed
	}

	return nil
}

func (b *broker) streamKey(topic string) string {
	return fmt.Sprintf(b.opts.streamKeyFormat, topic)
}

func (b *broker) newConsumerID(topic string) string {
	prefix := b.opts.consumerPrefix
	if prefix != "" {
		prefix += "-"
	}

	return fmt.Sprintf("consumer-%s%s-%s",
		prefix, topic, uuid.New().String())
}

func (b *broker) cleanupConsumer(
	stream, group, consumer string,
) {
	ctx, cancel := context.WithTimeout(
		context.Background(), cleanupTimeout,
	)
	defer cancel()

	_ = b.client.XGroupDelConsumer(ctx, stream, group, consumer).Err()
}

func (b *broker) groupExists(
	ctx context.Context, stream, group string,
) (bool, error) {
	groups, err := b.client.XInfoGroups(ctx, stream).Result()
	if err != nil {
		if isStreamNotFound(err) {
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

func extractPayload(values map[string]interface{}) ([]byte, error) {
	raw, ok := values[payloadField].(string)
	if !ok {
		return nil, ErrInvalidPayload
	}

	return []byte(raw), nil
}

func isStreamNotFound(err error) bool {
	if errors.Is(err, redis.Nil) {
		return true
	}

	return strings.Contains(err.Error(), "no such key")
}

func isGroupExistsErr(err error) bool {
	if err == nil {
		return false
	}

	msg := err.Error()

	return strings.HasPrefix(msg, "BUSYGROUP") ||
		strings.Contains(msg, "already exists")
}
