package redis

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/shuldan/queue"
)

type mockRedisClient struct {
	redis.UniversalClient
	xAddFn                 func(ctx context.Context, a *redis.XAddArgs) *redis.StringCmd
	pingFn                 func(ctx context.Context) *redis.StatusCmd
	xInfoGroupsFn          func(ctx context.Context, key string) *redis.XInfoGroupsCmd
	xGroupCreateMkStreamFn func(ctx context.Context, stream, group, start string) *redis.StatusCmd
	xReadGroupFn           func(ctx context.Context, a *redis.XReadGroupArgs) *redis.XStreamSliceCmd
	xAckFn                 func(ctx context.Context, stream, group string, ids ...string) *redis.IntCmd
	xPendingExtFn          func(ctx context.Context, a *redis.XPendingExtArgs) *redis.XPendingExtCmd
	xClaimFn               func(ctx context.Context, a *redis.XClaimArgs) *redis.XMessageSliceCmd
	xGroupDelConsumerFn    func(ctx context.Context, stream, group, consumer string) *redis.IntCmd
}

func (m *mockRedisClient) XAdd(ctx context.Context, a *redis.XAddArgs) *redis.StringCmd {
	if m.xAddFn != nil {
		return m.xAddFn(ctx, a)
	}
	cmd := redis.NewStringCmd(ctx)
	cmd.SetVal("1-0")
	return cmd
}

func (m *mockRedisClient) Ping(ctx context.Context) *redis.StatusCmd {
	if m.pingFn != nil {
		return m.pingFn(ctx)
	}
	cmd := redis.NewStatusCmd(ctx)
	cmd.SetVal("PONG")
	return cmd
}

func (m *mockRedisClient) XInfoGroups(ctx context.Context, key string) *redis.XInfoGroupsCmd {
	if m.xInfoGroupsFn != nil {
		return m.xInfoGroupsFn(ctx, key)
	}
	cmd := new(redis.XInfoGroupsCmd)
	return cmd
}

func (m *mockRedisClient) XGroupCreateMkStream(
	ctx context.Context, stream, group, start string,
) *redis.StatusCmd {
	if m.xGroupCreateMkStreamFn != nil {
		return m.xGroupCreateMkStreamFn(ctx, stream, group, start)
	}
	cmd := redis.NewStatusCmd(ctx)
	cmd.SetVal("OK")
	return cmd
}

func (m *mockRedisClient) XReadGroup(
	ctx context.Context, a *redis.XReadGroupArgs,
) *redis.XStreamSliceCmd {
	if m.xReadGroupFn != nil {
		return m.xReadGroupFn(ctx, a)
	}
	cmd := new(redis.XStreamSliceCmd)
	cmd.SetErr(redis.Nil)
	return cmd
}

func (m *mockRedisClient) XAck(
	ctx context.Context, stream, group string, ids ...string,
) *redis.IntCmd {
	if m.xAckFn != nil {
		return m.xAckFn(ctx, stream, group, ids...)
	}
	cmd := redis.NewIntCmd(ctx)
	cmd.SetVal(1)
	return cmd
}

func (m *mockRedisClient) XPendingExt(
	ctx context.Context, a *redis.XPendingExtArgs,
) *redis.XPendingExtCmd {
	if m.xPendingExtFn != nil {
		return m.xPendingExtFn(ctx, a)
	}
	cmd := new(redis.XPendingExtCmd)
	return cmd
}

func (m *mockRedisClient) XClaim(
	ctx context.Context, a *redis.XClaimArgs,
) *redis.XMessageSliceCmd {
	if m.xClaimFn != nil {
		return m.xClaimFn(ctx, a)
	}
	cmd := new(redis.XMessageSliceCmd)
	return cmd
}

func (m *mockRedisClient) XGroupDelConsumer(
	ctx context.Context, stream, group, consumer string,
) *redis.IntCmd {
	if m.xGroupDelConsumerFn != nil {
		return m.xGroupDelConsumerFn(ctx, stream, group, consumer)
	}
	cmd := redis.NewIntCmd(ctx)
	cmd.SetVal(0)
	return cmd
}

func defaultMock() *mockRedisClient {
	return &mockRedisClient{
		xInfoGroupsFn: func(ctx context.Context, key string) *redis.XInfoGroupsCmd {
			cmd := new(redis.XInfoGroupsCmd)
			cmd.SetErr(redis.Nil)
			return cmd
		},
	}
}

func readyReadGroupMock(ready chan struct{}) func(ctx context.Context, a *redis.XReadGroupArgs) *redis.XStreamSliceCmd {
	return func(ctx context.Context, a *redis.XReadGroupArgs) *redis.XStreamSliceCmd {
		select {
		case ready <- struct{}{}:
		default:
		}
		cmd := new(redis.XStreamSliceCmd)
		cmd.SetErr(redis.Nil)
		return cmd
	}
}

func TestExtractPayload_Valid(t *testing.T) {
	t.Parallel()
	vals := map[string]interface{}{payloadField: "hello"}
	data, err := extractPayload(vals)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if string(data) != "hello" {
		t.Errorf("expected 'hello', got %q", string(data))
	}
}

func TestExtractPayload_Missing(t *testing.T) {
	t.Parallel()
	vals := map[string]interface{}{"other": "value"}
	_, err := extractPayload(vals)
	if !errors.Is(err, ErrInvalidPayload) {
		t.Errorf("expected ErrInvalidPayload, got %v", err)
	}
}

func TestExtractPayload_WrongType(t *testing.T) {
	t.Parallel()
	vals := map[string]interface{}{payloadField: 12345}
	_, err := extractPayload(vals)
	if !errors.Is(err, ErrInvalidPayload) {
		t.Errorf("expected ErrInvalidPayload, got %v", err)
	}
}

func TestExtractPayload_NilMap(t *testing.T) {
	t.Parallel()
	_, err := extractPayload(nil)
	if !errors.Is(err, ErrInvalidPayload) {
		t.Errorf("expected ErrInvalidPayload, got %v", err)
	}
}

func TestIsStreamNotFound_AllBranches(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"redis_nil", redis.Nil, true},
		{"no_such_key", errors.New("ERR no such key"), true},
		{"other_error", errors.New("some other error"), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := isStreamNotFound(tc.err)
			if got != tc.want {
				t.Errorf("expected %v, got %v", tc.want, got)
			}
		})
	}
}

func TestIsGroupExistsErr_AllBranches(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"busygroup", errors.New("BUSYGROUP Consumer Group name already exists"), true},
		{"already_exists", errors.New("group already exists"), true},
		{"other", errors.New("random error"), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := isGroupExistsErr(tc.err)
			if got != tc.want {
				t.Errorf("expected %v, got %v", tc.want, got)
			}
		})
	}
}

func TestBroker_StreamKey(t *testing.T) {
	t.Parallel()
	b := &broker{opts: &options{streamKeyFormat: "mystream:%s"}}
	got := b.streamKey("orders")
	if got != "mystream:orders" {
		t.Errorf("expected 'mystream:orders', got %q", got)
	}
}

func TestBroker_NewConsumerID_WithPrefix(t *testing.T) {
	t.Parallel()
	b := &broker{opts: &options{consumerPrefix: "svc"}}
	id := b.newConsumerID("topic1")
	if len(id) < len("consumer-svc-") {
		t.Fatalf("consumer ID too short: %q", id)
	}
	if id[:len("consumer-svc-")] != "consumer-svc-" {
		t.Errorf("expected prefix 'consumer-svc-', got %q", id)
	}
}

func TestBroker_NewConsumerID_NoPrefix(t *testing.T) {
	t.Parallel()
	b := &broker{opts: &options{consumerPrefix: ""}}
	id := b.newConsumerID("topic1")
	if len(id) < len("consumer-") {
		t.Fatalf("consumer ID too short: %q", id)
	}
	if id[:len("consumer-")] != "consumer-" {
		t.Errorf("expected prefix 'consumer-', got %q", id)
	}
}

func TestBroker_CheckClosed_Open(t *testing.T) {
	t.Parallel()
	b := &broker{done: make(chan struct{}), opts: &options{}}
	if err := b.checkClosed(); err != nil {
		t.Errorf("expected nil, got %v", err)
	}
}

func TestBroker_CheckClosed_Closed(t *testing.T) {
	t.Parallel()
	b := &broker{done: make(chan struct{}), closed: true, opts: &options{}}
	if err := b.checkClosed(); err == nil {
		t.Fatal("expected error for closed broker")
	}
}

func TestBroker_Close_Idempotent(t *testing.T) {
	t.Parallel()
	b := &broker{done: make(chan struct{}), opts: &options{}}
	if err := b.Close(); err != nil {
		t.Fatalf("first close: %v", err)
	}
	if err := b.Close(); err != nil {
		t.Fatalf("second close: %v", err)
	}
}

func TestBroker_BuildXAddArgs_WithMaxLen(t *testing.T) {
	t.Parallel()
	b := &broker{opts: &options{maxStreamLength: 1000, approximateTrim: true}}
	args := b.buildXAddArgs("stream:test", []byte("data"))
	if args.MaxLen != 1000 {
		t.Errorf("expected MaxLen 1000, got %d", args.MaxLen)
	}
	if !args.Approx {
		t.Error("expected Approx true")
	}
}

func TestBroker_BuildXAddArgs_NoMaxLen(t *testing.T) {
	t.Parallel()
	b := &broker{opts: &options{maxStreamLength: 0}}
	args := b.buildXAddArgs("stream:test", []byte("data"))
	if args.MaxLen != 0 {
		t.Errorf("expected MaxLen 0, got %d", args.MaxLen)
	}
}

func TestBroker_ConsumerNames(t *testing.T) {
	t.Parallel()
	b := &broker{opts: &options{
		streamKeyFormat: "s:%s", consumerGroup: "grp", consumerPrefix: "",
	}}
	stream, group, consumer := b.consumerNames("orders")
	if stream != "s:orders" {
		t.Errorf("expected 's:orders', got %q", stream)
	}
	if group != "grp:orders" {
		t.Errorf("expected 'grp:orders', got %q", group)
	}
	if consumer == "" {
		t.Error("expected non-empty consumer")
	}
}

func TestHandleReadError_ContextCancelled(t *testing.T) {
	t.Parallel()
	b := &broker{opts: &options{}}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	b.handleReadError(ctx, errors.New("some error"))
}

func TestHandleReadError_RedisNil(t *testing.T) {
	t.Parallel()
	b := &broker{opts: &options{}}
	start := time.Now()
	b.handleReadError(context.Background(), redis.Nil)
	if time.Since(start) > 100*time.Millisecond {
		t.Error("redis.Nil should return immediately without backoff")
	}
}

func TestHandleReadError_NormalError(t *testing.T) {
	t.Parallel()
	b := &broker{opts: &options{}}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	b.handleReadError(ctx, errors.New("some error"))
	if time.Since(start) < 40*time.Millisecond {
		t.Error("expected backoff delay")
	}
}

func TestErrors_Defined(t *testing.T) {
	t.Parallel()
	errs := []error{ErrInvalidPayload, ErrProduceFailed, ErrConsumeSetupFailed, ErrGroupCheckFailed}
	for _, e := range errs {
		if e == nil {
			t.Error("expected non-nil error")
		}
	}
}

func TestBroker_Produce_Success(t *testing.T) {
	t.Parallel()
	client := defaultMock()
	b := New(client).(*broker)
	defer b.Close()
	err := b.Produce(context.Background(), "topic", []byte("data"))
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
}

func TestBroker_Produce_Closed(t *testing.T) {
	t.Parallel()
	b := New(defaultMock()).(*broker)
	b.Close()
	err := b.Produce(context.Background(), "topic", []byte("data"))
	if !errors.Is(err, queue.ErrBrokerClosed) {
		t.Errorf("expected ErrBrokerClosed, got %v", err)
	}
}

func TestBroker_Produce_XAddError(t *testing.T) {
	t.Parallel()
	client := defaultMock()
	client.xAddFn = func(ctx context.Context, a *redis.XAddArgs) *redis.StringCmd {
		cmd := redis.NewStringCmd(ctx)
		cmd.SetErr(errors.New("NOPERM"))
		return cmd
	}
	b := New(client).(*broker)
	defer b.Close()
	err := b.Produce(context.Background(), "topic", []byte("data"))
	if !errors.Is(err, ErrProduceFailed) {
		t.Errorf("expected ErrProduceFailed, got %v", err)
	}
}

func TestBroker_Ping_Success(t *testing.T) {
	t.Parallel()
	b := New(defaultMock()).(*broker)
	defer b.Close()
	if err := b.Ping(context.Background()); err != nil {
		t.Errorf("expected nil, got %v", err)
	}
}

func TestBroker_Ping_Error(t *testing.T) {
	t.Parallel()
	client := defaultMock()
	client.pingFn = func(ctx context.Context) *redis.StatusCmd {
		cmd := redis.NewStatusCmd(ctx)
		cmd.SetErr(errors.New("connection refused"))
		return cmd
	}
	b := New(client).(*broker)
	defer b.Close()
	if err := b.Ping(context.Background()); err == nil {
		t.Error("expected error")
	}
}

func TestBroker_GroupExists_Found(t *testing.T) {
	t.Parallel()
	client := defaultMock()
	client.xInfoGroupsFn = func(ctx context.Context, key string) *redis.XInfoGroupsCmd {
		cmd := new(redis.XInfoGroupsCmd)
		cmd.SetVal([]redis.XInfoGroup{{Name: "mygroup"}})
		return cmd
	}
	b := New(client).(*broker)
	defer b.Close()
	exists, err := b.groupExists(context.Background(), "stream", "mygroup")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !exists {
		t.Error("expected group to exist")
	}
}

func TestBroker_GroupExists_NotFound(t *testing.T) {
	t.Parallel()
	client := defaultMock()
	client.xInfoGroupsFn = func(ctx context.Context, key string) *redis.XInfoGroupsCmd {
		cmd := new(redis.XInfoGroupsCmd)
		cmd.SetVal([]redis.XInfoGroup{{Name: "other"}})
		return cmd
	}
	b := New(client).(*broker)
	defer b.Close()
	exists, err := b.groupExists(context.Background(), "stream", "mygroup")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if exists {
		t.Error("expected group not to exist")
	}
}

func TestBroker_GroupExists_StreamNotFound(t *testing.T) {
	t.Parallel()
	client := defaultMock()
	client.xInfoGroupsFn = func(ctx context.Context, key string) *redis.XInfoGroupsCmd {
		cmd := new(redis.XInfoGroupsCmd)
		cmd.SetErr(redis.Nil)
		return cmd
	}
	b := New(client).(*broker)
	defer b.Close()
	exists, err := b.groupExists(context.Background(), "stream", "mygroup")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if exists {
		t.Error("expected false for non-existent stream")
	}
}

func TestBroker_GroupExists_OtherError(t *testing.T) {
	t.Parallel()
	client := defaultMock()
	client.xInfoGroupsFn = func(ctx context.Context, key string) *redis.XInfoGroupsCmd {
		cmd := new(redis.XInfoGroupsCmd)
		cmd.SetErr(errors.New("connection refused"))
		return cmd
	}
	b := New(client).(*broker)
	defer b.Close()
	_, err := b.groupExists(context.Background(), "stream", "mygroup")
	if err == nil {
		t.Error("expected error")
	}
}

func TestBroker_EnsureGroup_AlreadyExists(t *testing.T) {
	t.Parallel()
	client := defaultMock()
	client.xInfoGroupsFn = func(ctx context.Context, key string) *redis.XInfoGroupsCmd {
		cmd := new(redis.XInfoGroupsCmd)
		cmd.SetVal([]redis.XInfoGroup{{Name: "grp:topic"}})
		return cmd
	}
	b := New(client).(*broker)
	defer b.Close()
	err := b.ensureGroup(context.Background(), "stream:topic", "grp:topic")
	if err != nil {
		t.Errorf("expected nil, got %v", err)
	}
}

func TestBroker_EnsureGroup_CreateNew(t *testing.T) {
	t.Parallel()
	b := New(defaultMock()).(*broker)
	defer b.Close()
	err := b.ensureGroup(context.Background(), "stream:topic", "grp:topic")
	if err != nil {
		t.Errorf("expected nil, got %v", err)
	}
}

func TestBroker_EnsureGroup_BusyGroup(t *testing.T) {
	t.Parallel()
	client := defaultMock()
	client.xGroupCreateMkStreamFn = func(ctx context.Context, stream, group, start string) *redis.StatusCmd {
		cmd := redis.NewStatusCmd(ctx)
		cmd.SetErr(errors.New("BUSYGROUP Consumer Group name already exists"))
		return cmd
	}
	b := New(client).(*broker)
	defer b.Close()
	err := b.ensureGroup(context.Background(), "stream", "group")
	if err != nil {
		t.Errorf("BUSYGROUP should be ignored, got %v", err)
	}
}

func TestBroker_EnsureGroup_CreateError(t *testing.T) {
	t.Parallel()
	client := defaultMock()
	client.xGroupCreateMkStreamFn = func(ctx context.Context, stream, group, start string) *redis.StatusCmd {
		cmd := redis.NewStatusCmd(ctx)
		cmd.SetErr(errors.New("NOPERM"))
		return cmd
	}
	b := New(client).(*broker)
	defer b.Close()
	err := b.ensureGroup(context.Background(), "stream", "group")
	if !errors.Is(err, ErrConsumeSetupFailed) {
		t.Errorf("expected ErrConsumeSetupFailed, got %v", err)
	}
}

func TestBroker_EnsureGroup_CheckError(t *testing.T) {
	t.Parallel()
	client := defaultMock()
	client.xInfoGroupsFn = func(ctx context.Context, key string) *redis.XInfoGroupsCmd {
		cmd := new(redis.XInfoGroupsCmd)
		cmd.SetErr(errors.New("connection refused"))
		return cmd
	}
	b := New(client).(*broker)
	defer b.Close()
	err := b.ensureGroup(context.Background(), "stream", "group")
	if !errors.Is(err, ErrGroupCheckFailed) {
		t.Errorf("expected ErrGroupCheckFailed, got %v", err)
	}
}

func TestBroker_HandleStreamMessage_ValidPayload(t *testing.T) {
	t.Parallel()
	var acked atomic.Bool
	client := defaultMock()
	client.xAckFn = func(ctx context.Context, stream, group string, ids ...string) *redis.IntCmd {
		acked.Store(true)
		cmd := redis.NewIntCmd(ctx)
		cmd.SetVal(1)
		return cmd
	}
	b := New(client).(*broker)
	defer b.Close()
	msg := redis.XMessage{ID: "1-0", Values: map[string]interface{}{"payload": "data"}}
	var received string
	b.handleStreamMessage(context.Background(), "s", "g", msg, func(data []byte) error {
		received = string(data)
		return nil
	})
	if received != "data" {
		t.Errorf("expected 'data', got %q", received)
	}
	if !acked.Load() {
		t.Error("expected XAck to be called")
	}
}

func TestBroker_HandleStreamMessage_InvalidPayload(t *testing.T) {
	t.Parallel()
	var acked atomic.Bool
	client := defaultMock()
	client.xAckFn = func(ctx context.Context, stream, group string, ids ...string) *redis.IntCmd {
		acked.Store(true)
		cmd := redis.NewIntCmd(ctx)
		cmd.SetVal(1)
		return cmd
	}
	b := New(client).(*broker)
	defer b.Close()
	msg := redis.XMessage{ID: "1-0", Values: map[string]interface{}{"wrong": "field"}}
	b.handleStreamMessage(context.Background(), "s", "g", msg, func([]byte) error {
		t.Error("handler should not be called")
		return nil
	})
	if !acked.Load() {
		t.Error("expected XAck for invalid payload")
	}
}

func TestBroker_HandleStreamMessage_HandlerError(t *testing.T) {
	t.Parallel()
	var acked atomic.Bool
	client := defaultMock()
	client.xAckFn = func(ctx context.Context, stream, group string, ids ...string) *redis.IntCmd {
		acked.Store(true)
		cmd := redis.NewIntCmd(ctx)
		cmd.SetVal(1)
		return cmd
	}
	b := New(client).(*broker)
	defer b.Close()
	msg := redis.XMessage{ID: "1-0", Values: map[string]interface{}{"payload": "data"}}
	b.handleStreamMessage(context.Background(), "s", "g", msg, func([]byte) error {
		return errors.New("fail")
	})
	if acked.Load() {
		t.Error("XAck should NOT be called when handler fails")
	}
}

func TestBroker_ProcessNewMessage_Success(t *testing.T) {
	t.Parallel()
	var handlerCalled atomic.Bool
	client := defaultMock()
	client.xReadGroupFn = func(ctx context.Context, a *redis.XReadGroupArgs) *redis.XStreamSliceCmd {
		cmd := new(redis.XStreamSliceCmd)
		cmd.SetVal([]redis.XStream{{
			Stream: "s",
			Messages: []redis.XMessage{{
				ID:     "1-0",
				Values: map[string]interface{}{"payload": "hello"},
			}},
		}})
		return cmd
	}
	b := New(client, WithBlockTimeout(time.Millisecond)).(*broker)
	defer b.Close()
	b.processNewMessage(context.Background(), "s", "g", "c", func(data []byte) error {
		handlerCalled.Store(true)
		return nil
	})
	if !handlerCalled.Load() {
		t.Error("expected handler to be called")
	}
}

func TestBroker_ProcessNewMessage_EmptyResult(t *testing.T) {
	t.Parallel()
	client := defaultMock()
	client.xReadGroupFn = func(ctx context.Context, a *redis.XReadGroupArgs) *redis.XStreamSliceCmd {
		cmd := new(redis.XStreamSliceCmd)
		cmd.SetVal([]redis.XStream{})
		return cmd
	}
	b := New(client, WithBlockTimeout(time.Millisecond)).(*broker)
	defer b.Close()
	b.processNewMessage(context.Background(), "s", "g", "c", func([]byte) error {
		t.Error("handler should not be called on empty result")
		return nil
	})
}

func TestBroker_ProcessNewMessage_EmptyMessages(t *testing.T) {
	t.Parallel()
	client := defaultMock()
	client.xReadGroupFn = func(ctx context.Context, a *redis.XReadGroupArgs) *redis.XStreamSliceCmd {
		cmd := new(redis.XStreamSliceCmd)
		cmd.SetVal([]redis.XStream{{Stream: "s", Messages: []redis.XMessage{}}})
		return cmd
	}
	b := New(client, WithBlockTimeout(time.Millisecond)).(*broker)
	defer b.Close()
	b.processNewMessage(context.Background(), "s", "g", "c", func([]byte) error {
		t.Error("handler should not be called on empty messages")
		return nil
	})
}

func TestBroker_ProcessNewMessage_ReadError(t *testing.T) {
	t.Parallel()
	client := defaultMock()
	client.xReadGroupFn = func(ctx context.Context, a *redis.XReadGroupArgs) *redis.XStreamSliceCmd {
		cmd := new(redis.XStreamSliceCmd)
		cmd.SetErr(errors.New("connection error"))
		return cmd
	}
	b := New(client, WithBlockTimeout(time.Millisecond)).(*broker)
	defer b.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	b.processNewMessage(ctx, "s", "g", "c", func([]byte) error { return nil })
}

func TestBroker_CleanupConsumer(t *testing.T) {
	t.Parallel()
	var called atomic.Bool
	client := defaultMock()
	client.xGroupDelConsumerFn = func(ctx context.Context, stream, group, consumer string) *redis.IntCmd {
		called.Store(true)
		cmd := redis.NewIntCmd(ctx)
		cmd.SetVal(0)
		return cmd
	}
	b := New(client).(*broker)
	defer b.Close()
	b.cleanupConsumer("stream", "group", "consumer")
	if !called.Load() {
		t.Error("expected XGroupDelConsumer to be called")
	}
}

func TestBroker_FindPendingMessages_Success(t *testing.T) {
	t.Parallel()
	client := defaultMock()
	client.xPendingExtFn = func(ctx context.Context, a *redis.XPendingExtArgs) *redis.XPendingExtCmd {
		cmd := new(redis.XPendingExtCmd)
		cmd.SetVal([]redis.XPendingExt{{ID: "1-0", Consumer: "other"}})
		return cmd
	}
	b := New(client).(*broker)
	defer b.Close()
	msgs := b.findPendingMessages(context.Background(), "stream", "group")
	if len(msgs) != 1 {
		t.Errorf("expected 1 pending message, got %d", len(msgs))
	}
}

func TestBroker_FindPendingMessages_Error(t *testing.T) {
	t.Parallel()
	client := defaultMock()
	client.xPendingExtFn = func(ctx context.Context, a *redis.XPendingExtArgs) *redis.XPendingExtCmd {
		cmd := new(redis.XPendingExtCmd)
		cmd.SetErr(errors.New("error"))
		return cmd
	}
	b := New(client).(*broker)
	defer b.Close()
	msgs := b.findPendingMessages(context.Background(), "stream", "group")
	if msgs != nil {
		t.Errorf("expected nil, got %v", msgs)
	}
}

func TestBroker_ClaimAndHandle_Success(t *testing.T) {
	t.Parallel()
	var handlerCalled atomic.Bool
	client := defaultMock()
	client.xClaimFn = func(ctx context.Context, a *redis.XClaimArgs) *redis.XMessageSliceCmd {
		cmd := new(redis.XMessageSliceCmd)
		cmd.SetVal([]redis.XMessage{{
			ID:     "1-0",
			Values: map[string]interface{}{"payload": "claimed"},
		}})
		return cmd
	}
	b := New(client).(*broker)
	defer b.Close()
	b.claimAndHandle(context.Background(), "s", "g", "c", "1-0", func(data []byte) error {
		handlerCalled.Store(true)
		return nil
	})
	if !handlerCalled.Load() {
		t.Error("expected handler to be called")
	}
}

func TestBroker_ClaimAndHandle_Error(t *testing.T) {
	t.Parallel()
	client := defaultMock()
	client.xClaimFn = func(ctx context.Context, a *redis.XClaimArgs) *redis.XMessageSliceCmd {
		cmd := new(redis.XMessageSliceCmd)
		cmd.SetErr(errors.New("claim error"))
		return cmd
	}
	b := New(client).(*broker)
	defer b.Close()
	b.claimAndHandle(context.Background(), "s", "g", "c", "1-0", func([]byte) error {
		t.Error("handler should not be called on claim error")
		return nil
	})
}

func TestBroker_ClaimStalledMessages(t *testing.T) {
	t.Parallel()
	var handlerCalled atomic.Bool
	client := defaultMock()
	client.xPendingExtFn = func(ctx context.Context, a *redis.XPendingExtArgs) *redis.XPendingExtCmd {
		cmd := new(redis.XPendingExtCmd)
		cmd.SetVal([]redis.XPendingExt{{ID: "1-0"}})
		return cmd
	}
	client.xClaimFn = func(ctx context.Context, a *redis.XClaimArgs) *redis.XMessageSliceCmd {
		cmd := new(redis.XMessageSliceCmd)
		cmd.SetVal([]redis.XMessage{{
			ID:     "1-0",
			Values: map[string]interface{}{"payload": "stalled"},
		}})
		return cmd
	}
	b := New(client).(*broker)
	defer b.Close()
	b.claimStalledMessages(context.Background(), "s", "g", "c", func([]byte) error {
		handlerCalled.Store(true)
		return nil
	})
	if !handlerCalled.Load() {
		t.Error("expected handler to be called")
	}
}

func TestBroker_Consume_EndToEnd(t *testing.T) {
	t.Parallel()
	var handlerCalled atomic.Bool
	ready := make(chan struct{}, 1)
	var readCount atomic.Int32
	client := defaultMock()
	client.xReadGroupFn = func(ctx context.Context, a *redis.XReadGroupArgs) *redis.XStreamSliceCmd {
		select {
		case ready <- struct{}{}:
		default:
		}
		cmd := new(redis.XStreamSliceCmd)
		if readCount.Add(1) == 1 {
			cmd.SetVal([]redis.XStream{{
				Stream: a.Streams[0],
				Messages: []redis.XMessage{{
					ID:     "1-0",
					Values: map[string]interface{}{"payload": "hello"},
				}},
			}})
		} else {
			cmd.SetErr(redis.Nil)
		}
		return cmd
	}
	b := New(client, WithClaim(false), WithBlockTimeout(time.Millisecond)).(*broker)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- b.Consume(ctx, "test-topic", func(data []byte) error {
			handlerCalled.Store(true)
			return nil
		})
	}()

	select {
	case <-ready:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for consumer to start")
	}

	time.Sleep(50 * time.Millisecond)
	cancel()
	err := <-errCh
	b.Close()

	if err != nil && !errors.Is(err, context.Canceled) {
		t.Errorf("unexpected error: %v", err)
	}
	if !handlerCalled.Load() {
		t.Error("expected handler to be called")
	}
}

func TestBroker_Consume_Closed(t *testing.T) {
	t.Parallel()
	b := New(defaultMock()).(*broker)
	b.Close()
	err := b.Consume(context.Background(), "t", func([]byte) error { return nil })
	if !errors.Is(err, queue.ErrBrokerClosed) {
		t.Errorf("expected ErrBrokerClosed, got %v", err)
	}
}

func TestBroker_Consume_EnsureGroupError(t *testing.T) {
	t.Parallel()
	client := defaultMock()
	client.xInfoGroupsFn = func(ctx context.Context, key string) *redis.XInfoGroupsCmd {
		cmd := new(redis.XInfoGroupsCmd)
		cmd.SetErr(errors.New("connection refused"))
		return cmd
	}
	b := New(client).(*broker)
	defer b.Close()
	err := b.Consume(context.Background(), "test", func([]byte) error { return nil })
	if !errors.Is(err, ErrGroupCheckFailed) {
		t.Errorf("expected ErrGroupCheckFailed, got %v", err)
	}
}

func TestBroker_Consume_BrokerCloseWhileRunning(t *testing.T) {
	t.Parallel()
	ready := make(chan struct{}, 1)
	client := defaultMock()
	client.xReadGroupFn = readyReadGroupMock(ready)
	b := New(client, WithClaim(false), WithBlockTimeout(time.Millisecond)).(*broker)

	errCh := make(chan error, 1)
	go func() {
		errCh <- b.Consume(context.Background(), "t", func([]byte) error { return nil })
	}()

	select {
	case <-ready:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for consumer to start")
	}

	b.Close()
	err := <-errCh
	if !errors.Is(err, queue.ErrBrokerClosed) {
		t.Errorf("expected ErrBrokerClosed, got %v", err)
	}
}

func TestBroker_Consume_WithClaim(t *testing.T) {
	t.Parallel()
	var claimHandlerCalled atomic.Bool
	ready := make(chan struct{}, 1)
	client := defaultMock()
	client.xReadGroupFn = readyReadGroupMock(ready)
	client.xPendingExtFn = func(ctx context.Context, a *redis.XPendingExtArgs) *redis.XPendingExtCmd {
		cmd := new(redis.XPendingExtCmd)
		cmd.SetVal([]redis.XPendingExt{{ID: "1-0"}})
		return cmd
	}
	client.xClaimFn = func(ctx context.Context, a *redis.XClaimArgs) *redis.XMessageSliceCmd {
		cmd := new(redis.XMessageSliceCmd)
		cmd.SetVal([]redis.XMessage{{
			ID:     "1-0",
			Values: map[string]interface{}{"payload": "claimed"},
		}})
		return cmd
	}
	b := New(client,
		WithClaim(true),
		WithClaimInterval(10*time.Millisecond),
		WithBlockTimeout(time.Millisecond),
	).(*broker)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- b.Consume(ctx, "topic", func(data []byte) error {
			claimHandlerCalled.Store(true)
			return nil
		})
	}()

	select {
	case <-ready:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for consumer to start")
	}

	time.Sleep(100 * time.Millisecond)
	cancel()
	<-errCh
	b.Close()

	if !claimHandlerCalled.Load() {
		t.Error("expected handler to be called via claim")
	}
}

func TestBroker_Produce_WithMaxStreamLength(t *testing.T) {
	t.Parallel()
	var capturedArgs *redis.XAddArgs
	client := defaultMock()
	client.xAddFn = func(ctx context.Context, a *redis.XAddArgs) *redis.StringCmd {
		capturedArgs = a
		cmd := redis.NewStringCmd(ctx)
		cmd.SetVal("1-0")
		return cmd
	}
	b := New(client, WithMaxStreamLength(500)).(*broker)
	defer b.Close()
	_ = b.Produce(context.Background(), "t", []byte("d"))
	if capturedArgs == nil {
		t.Fatal("XAdd was not called")
	}
	if capturedArgs.MaxLen != 500 {
		t.Errorf("expected MaxLen 500, got %d", capturedArgs.MaxLen)
	}
}
