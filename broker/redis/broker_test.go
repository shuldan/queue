package redis

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestBroker_Produce_Success(t *testing.T) {
	t.Parallel()

	mock := newMockCmdable()
	b := &broker{
		client: mock,
		opts:   &options{streamKeyFormat: "stream:%s"},
	}

	mock.expectXAdd("stream:test", "12345-0", nil)

	err := b.Produce(context.Background(), "test", []byte("hello"))
	if err != nil {
		t.Fatalf("Produce failed: %v", err)
	}

	mock.verifyXAddCalled(t, "stream:test")
}

func TestBroker_Produce_WithMaxStreamLength(t *testing.T) {
	t.Parallel()

	mock := newMockCmdable()
	b := &broker{
		client: mock,
		opts: &options{
			streamKeyFormat: "stream:%s",
			maxStreamLength: 1000,
			approximateTrim: true,
		},
	}

	mock.expectXAdd("stream:test", "12345-0", nil)

	err := b.Produce(context.Background(), "test", []byte("data"))
	if err != nil {
		t.Fatalf("Produce failed: %v", err)
	}

	mock.verifyXAddCalled(t, "stream:test")
}

func TestBroker_Produce_BrokerClosed(t *testing.T) {
	t.Parallel()

	mock := newMockCmdable()
	b := &broker{
		client: mock,
		opts:   &options{streamKeyFormat: "stream:%s"},
		closed: true,
	}

	err := b.Produce(context.Background(), "test", []byte("data"))
	if !errors.Is(err, ErrBrokerClosed) {
		t.Errorf("expected ErrBrokerClosed, got %v", err)
	}
}

func TestBroker_Produce_RedisError(t *testing.T) {
	t.Parallel()

	mock := newMockCmdable()
	b := &broker{
		client: mock,
		opts:   &options{streamKeyFormat: "stream:%s"},
	}

	expectedErr := errors.New("redis connection error")
	mock.expectXAdd("stream:test", "", expectedErr)

	err := b.Produce(context.Background(), "test", []byte("data"))
	if err == nil {
		t.Error("expected error, got nil")
	}
	if !errors.Is(err, ErrProduceFailed) {
		t.Errorf("expected ErrProduceFailed wrapper, got %v", err)
	}
}

func TestBroker_Consume_NewGroup(t *testing.T) {
	mock := newMockCmdable()
	b := &broker{
		client: mock,
		opts: &options{
			streamKeyFormat: "stream:%s",
			consumerGroup:   "consumers",
			claimInterval:   100 * time.Millisecond,
			blockTimeout:    10 * time.Millisecond,
			enableClaim:     false,
		},
		consumers: make(map[string][]context.CancelFunc),
	}

	mock.expectXInfoGroups("stream:test", []redis.XInfoGroup{}, redis.Nil)
	mock.expectXGroupCreateMkStream("stream:test", "consumers:test", "0", "OK", nil)
	mock.setXReadGroupAlwaysNil("stream:test", "consumers:test")

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	err := b.Consume(ctx, "test", func(data []byte) error {
		return nil
	})

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("expected DeadlineExceeded, got %v", err)
	}

	mock.verifyXInfoGroupsCalled(t, "stream:test")
	mock.verifyXGroupCreateCalled(t, "stream:test")
}

func TestBroker_Consume_ExistingGroup(t *testing.T) {
	mock := newMockCmdable()
	b := &broker{
		client: mock,
		opts: &options{
			streamKeyFormat: "stream:%s",
			consumerGroup:   "consumers",
			claimInterval:   100 * time.Millisecond,
			blockTimeout:    10 * time.Millisecond,
			enableClaim:     false,
		},
		consumers: make(map[string][]context.CancelFunc),
	}

	mock.expectXInfoGroups("stream:test", []redis.XInfoGroup{
		{Name: "consumers:test"},
	}, nil)

	message := redis.XMessage{
		ID:     "12345-0",
		Values: map[string]interface{}{"payload": `{"data":"aGVsbG8=","enqueued_at":"2025-01-01T00:00:00Z"}`},
	}
	mock.addXReadGroupMessage("stream:test", "consumers:test", message)

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

	select {
	case data := <-received:
		if string(data) != "hello" {
			t.Errorf("expected 'hello', got %q", string(data))
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timeout waiting for message")
	}

	cancel()

	select {
	case err := <-consumeDone:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("expected context.Canceled, got %v", err)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("Consume did not finish")
	}
}

func TestBroker_Consume_BrokerClosed(t *testing.T) {
	t.Parallel()

	mock := newMockCmdable()
	b := &broker{
		client:    mock,
		opts:      &options{},
		consumers: make(map[string][]context.CancelFunc),
		closed:    true,
	}

	err := b.Consume(context.Background(), "test", func(data []byte) error {
		return nil
	})

	if !errors.Is(err, ErrBrokerClosed) {
		t.Errorf("expected ErrBrokerClosed, got %v", err)
	}
}

func TestBroker_Consume_GroupCreateError(t *testing.T) {
	mock := newMockCmdable()
	b := &broker{
		client: mock,
		opts: &options{
			streamKeyFormat: "stream:%s",
			consumerGroup:   "consumers",
		},
		consumers: make(map[string][]context.CancelFunc),
	}

	mock.expectXInfoGroups("stream:test", []redis.XInfoGroup{}, redis.Nil)
	expectedErr := errors.New("permission denied")
	mock.expectXGroupCreateMkStream("stream:test", "consumers:test", "0", "", expectedErr)

	ctx := context.Background()
	err := b.Consume(ctx, "test", func(data []byte) error {
		return nil
	})

	if err == nil {
		t.Error("expected error, got nil")
	}
	if !errors.Is(err, ErrConsumeSetupFailed) {
		t.Errorf("expected ErrConsumeSetupFailed, got %v", err)
	}
}

func TestBroker_Consume_HandlerError(t *testing.T) {
	mock := newMockCmdable()
	b := &broker{
		client: mock,
		opts: &options{
			streamKeyFormat: "stream:%s",
			consumerGroup:   "consumers",
			claimInterval:   100 * time.Millisecond,
			blockTimeout:    10 * time.Millisecond,
			enableClaim:     false,
		},
		consumers: make(map[string][]context.CancelFunc),
	}

	mock.expectXInfoGroups("stream:test", []redis.XInfoGroup{{Name: "consumers:test"}}, nil)

	message := redis.XMessage{
		ID:     "12345-0",
		Values: map[string]interface{}{"payload": `{"data":"dGVzdA==","enqueued_at":"2025-01-01T00:00:00Z"}`},
	}
	mock.addXReadGroupMessage("stream:test", "consumers:test", message)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	handlerCalled := make(chan struct{})
	consumeDone := make(chan error, 1)

	go func() {
		consumeDone <- b.Consume(ctx, "test", func(data []byte) error {
			close(handlerCalled)
			return errors.New("processing error")
		})
	}()

	select {
	case <-handlerCalled:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("handler was not called")
	}

	cancel()

	select {
	case err := <-consumeDone:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("expected context.Canceled, got %v", err)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("Consume did not finish")
	}

	mock.verifyXAckNotCalled(t)
}

func TestBroker_Consume_InvalidPayload(t *testing.T) {
	mock := newMockCmdable()
	b := &broker{
		client: mock,
		opts: &options{
			streamKeyFormat: "stream:%s",
			consumerGroup:   "consumers",
			claimInterval:   100 * time.Millisecond,
			blockTimeout:    10 * time.Millisecond,
			enableClaim:     false,
		},
		consumers: make(map[string][]context.CancelFunc),
	}

	mock.expectXInfoGroups("stream:test", []redis.XInfoGroup{{Name: "consumers:test"}}, nil)

	message := redis.XMessage{
		ID:     "12345-0",
		Values: map[string]interface{}{"wrong_field": "data"},
	}
	mock.addXReadGroupMessage("stream:test", "consumers:test", message)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	handlerCalled := atomic.Bool{}
	err := b.Consume(ctx, "test", func(data []byte) error {
		handlerCalled.Store(true)
		return nil
	})

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("expected DeadlineExceeded, got %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	if handlerCalled.Load() {
		t.Error("handler should not be called for invalid payload")
	}

	mock.verifyXAckCalled(t, "stream:test", "12345-0")
}

func TestBroker_Close_Success(t *testing.T) {
	t.Parallel()

	mock := newMockCmdable()
	b := &broker{
		client:    mock,
		consumers: make(map[string][]context.CancelFunc),
		opts:      &options{},
	}

	cancelCalled := atomic.Int32{}
	cancel := func() {
		cancelCalled.Add(1)
	}

	b.trackConsumer("test", cancel)
	b.trackConsumer("test", cancel)

	err := b.Close()
	if err != nil {
		t.Errorf("Close failed: %v", err)
	}

	if !b.closed {
		t.Error("broker should be marked as closed")
	}

	if cancelCalled.Load() != 2 {
		t.Errorf("expected 2 cancel calls, got %d", cancelCalled.Load())
	}
}

func TestBroker_Close_AlreadyClosed(t *testing.T) {
	t.Parallel()

	mock := newMockCmdable()
	b := &broker{
		client:    mock,
		consumers: make(map[string][]context.CancelFunc),
		opts:      &options{},
		closed:    true,
	}

	err := b.Close()
	if err != nil {
		t.Errorf("Close on already closed broker should not error, got %v", err)
	}
}

func TestBroker_Close_WaitsForConsumers(t *testing.T) {
	mock := newMockCmdable()
	b := &broker{
		client:    mock,
		consumers: make(map[string][]context.CancelFunc),
		opts: &options{
			streamKeyFormat: "stream:%s",
			consumerGroup:   "consumers",
			claimInterval:   100 * time.Millisecond,
			blockTimeout:    10 * time.Millisecond,
			enableClaim:     false,
		},
	}

	mock.expectXInfoGroups("stream:test", []redis.XInfoGroup{{Name: "consumers:test"}}, nil)
	mock.setXReadGroupAlwaysNil("stream:test", "consumers:test")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	consumeStarted := make(chan struct{})
	go func() {
		close(consumeStarted)
		_ = b.Consume(ctx, "test", func(data []byte) error {
			return nil
		})
	}()

	<-consumeStarted
	time.Sleep(20 * time.Millisecond)

	closeDone := make(chan struct{})
	go func() {
		_ = b.Close()
		close(closeDone)
	}()

	select {
	case <-closeDone:
	case <-time.After(200 * time.Millisecond):
		t.Error("Close did not complete in time")
	}
}

func TestBroker_GroupExists_StreamNotFound(t *testing.T) {
	t.Parallel()

	mock := newMockCmdable()
	b := &broker{client: mock, opts: &options{}}

	mock.expectXInfoGroups("stream:test", nil, redis.Nil)

	exists, err := b.groupExists(context.Background(), "stream:test", "group")
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if exists {
		t.Error("expected group to not exist")
	}
}

func TestBroker_GroupExists_GroupFound(t *testing.T) {
	t.Parallel()

	mock := newMockCmdable()
	b := &broker{client: mock, opts: &options{}}

	groups := []redis.XInfoGroup{
		{Name: "group1"},
		{Name: "group2"},
	}
	mock.expectXInfoGroups("stream:test", groups, nil)

	exists, err := b.groupExists(context.Background(), "stream:test", "group2")
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if !exists {
		t.Error("expected group to exist")
	}
}

func TestBroker_GroupExists_GroupNotFound(t *testing.T) {
	t.Parallel()

	mock := newMockCmdable()
	b := &broker{client: mock, opts: &options{}}

	groups := []redis.XInfoGroup{{Name: "group1"}}
	mock.expectXInfoGroups("stream:test", groups, nil)

	exists, err := b.groupExists(context.Background(), "stream:test", "group2")
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if exists {
		t.Error("expected group to not exist")
	}
}

func TestBroker_EncodeDecodeMessage(t *testing.T) {
	t.Parallel()

	b := &broker{}

	original := redisStreamMessage{
		Data:       []byte("test data"),
		EnqueuedAt: "2025-01-01T00:00:00Z",
	}

	encoded, err := b.encodeMessage(original)
	if err != nil {
		t.Fatalf("encode failed: %v", err)
	}

	var decoded redisStreamMessage
	err = b.decodeMessage(encoded, &decoded)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	if string(decoded.Data) != string(original.Data) {
		t.Errorf("data mismatch: expected %q, got %q", original.Data, decoded.Data)
	}
	if decoded.EnqueuedAt != original.EnqueuedAt {
		t.Errorf("timestamp mismatch: expected %q, got %q", original.EnqueuedAt, decoded.EnqueuedAt)
	}
}

func TestBroker_DecodeMessage_MissingPayload(t *testing.T) {
	t.Parallel()

	b := &broker{}

	values := map[string]interface{}{"wrong_key": "value"}
	var msg redisStreamMessage

	err := b.decodeMessage(values, &msg)
	if !errors.Is(err, ErrInvalidPayload) {
		t.Errorf("expected ErrInvalidPayload, got %v", err)
	}
}

func TestBroker_NewConsumerID_NoPrefix(t *testing.T) {
	t.Parallel()

	b := &broker{opts: &options{consumerPrefix: ""}}

	id1 := b.newConsumerID("test")
	id2 := b.newConsumerID("test")

	if id1 == id2 {
		t.Error("consumer IDs should be unique")
	}

	if id1[:9] != "consumer-" {
		t.Errorf("expected consumer ID to start with 'consumer-', got %q", id1)
	}
}

func TestBroker_NewConsumerID_WithPrefix(t *testing.T) {
	t.Parallel()

	b := &broker{opts: &options{consumerPrefix: "app"}}

	id := b.newConsumerID("test")

	if id[:13] != "consumer-app-" {
		t.Errorf("expected consumer ID to start with 'consumer-app-', got %q", id)
	}
}

func TestIsGroupExists(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{"nil_error", nil, false},
		{"busygroup_error", errors.New("BUSYGROUP Consumer Group name already exists"), true},
		{"already_exists", errors.New("group already exists"), true},
		{"other_error", errors.New("some other error"), false},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := isGroupExists(tt.err)
			if result != tt.expected {
				t.Errorf("expected %v, got %v", tt.expected, result)
			}
		})
	}
}

type xPendingExtResponse struct {
	result []redis.XPendingExt
	err    error
}

type xClaimResponse struct {
	result []redis.XMessage
	err    error
}

type mockCmdable struct {
	redis.UniversalClient
	mu                sync.Mutex
	xAddCalls         []xAddCall
	xReadGroupCalls   []xReadGroupCall
	xInfoGroupsCalls  []string
	xGroupCreateCalls []xGroupCreateCall
	xAckCalls         []xAckCall
	xAddResp          map[string]xAddResponse
	xReadGroupResp    map[string]*xReadGroupResponse
	xInfoGroupsResp   map[string]xInfoGroupsResponse
	xGroupCreateResp  map[string]xGroupCreateResponse
	xPendingExtResp   map[string]xPendingExtResponse
	xClaimResp        map[string]xClaimResponse
}

type xAddCall struct {
	stream string
	args   *redis.XAddArgs
}

type xAddResponse struct {
	result string
	err    error
}

type xReadGroupCall struct {
	stream string
	group  string
}

type xReadGroupResponse struct {
	messages     []redis.XMessage
	messageIndex int
	alwaysNil    bool
	err          error
}

type xInfoGroupsResponse struct {
	result []redis.XInfoGroup
	err    error
}

type xGroupCreateCall struct {
	stream string
	group  string
	id     string
}

type xGroupCreateResponse struct {
	result string
	err    error
}

type xAckCall struct {
	stream string
	group  string
	ids    []string
}

func newMockCmdable() *mockCmdable {
	return &mockCmdable{
		xAddResp:         make(map[string]xAddResponse),
		xReadGroupResp:   make(map[string]*xReadGroupResponse),
		xInfoGroupsResp:  make(map[string]xInfoGroupsResponse),
		xGroupCreateResp: make(map[string]xGroupCreateResponse),
		xPendingExtResp:  make(map[string]xPendingExtResponse),
		xClaimResp:       make(map[string]xClaimResponse),
	}
}

func (m *mockCmdable) expectXAdd(stream, result string, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.xAddResp[stream] = xAddResponse{result: result, err: err}
}

func (m *mockCmdable) addXReadGroupMessage(stream, group string, message redis.XMessage) {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := stream + ":" + group
	if m.xReadGroupResp[key] == nil {
		m.xReadGroupResp[key] = &xReadGroupResponse{}
	}
	m.xReadGroupResp[key].messages = append(m.xReadGroupResp[key].messages, message)
}

func (m *mockCmdable) setXReadGroupAlwaysNil(stream, group string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := stream + ":" + group
	if m.xReadGroupResp[key] == nil {
		m.xReadGroupResp[key] = &xReadGroupResponse{}
	}
	m.xReadGroupResp[key].alwaysNil = true
}

func (m *mockCmdable) expectXInfoGroups(stream string, result []redis.XInfoGroup, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.xInfoGroupsResp[stream] = xInfoGroupsResponse{result: result, err: err}
}

func (m *mockCmdable) expectXGroupCreateMkStream(stream, group, id, result string, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := stream + ":" + group + ":" + id
	m.xGroupCreateResp[key] = xGroupCreateResponse{result: result, err: err}
}

func (m *mockCmdable) expectXAck(_, _ string, _ []string, _ int64, _ error) {
}

func (m *mockCmdable) XAdd(ctx context.Context, a *redis.XAddArgs) *redis.StringCmd {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.xAddCalls = append(m.xAddCalls, xAddCall{stream: a.Stream, args: a})

	cmd := redis.NewStringCmd(ctx)
	if resp, ok := m.xAddResp[a.Stream]; ok {
		if resp.err != nil {
			cmd.SetErr(resp.err)
		} else {
			cmd.SetVal(resp.result)
		}
	} else {
		cmd.SetErr(redis.Nil)
	}
	return cmd
}

func (m *mockCmdable) XReadGroup(ctx context.Context, a *redis.XReadGroupArgs) *redis.XStreamSliceCmd {
	m.mu.Lock()
	defer m.mu.Unlock()

	if len(a.Streams) > 0 {
		m.xReadGroupCalls = append(m.xReadGroupCalls, xReadGroupCall{
			stream: a.Streams[0],
			group:  a.Group,
		})
	}

	cmd := redis.NewXStreamSliceCmd(ctx)
	if len(a.Streams) > 0 {
		key := a.Streams[0] + ":" + a.Group
		if resp, ok := m.xReadGroupResp[key]; ok {
			if resp.alwaysNil {
				cmd.SetErr(redis.Nil)
				return cmd
			}
			if resp.err != nil {
				cmd.SetErr(resp.err)
				return cmd
			}
			if resp.messageIndex < len(resp.messages) {
				msg := resp.messages[resp.messageIndex]
				resp.messageIndex++
				cmd.SetVal([]redis.XStream{{Messages: []redis.XMessage{msg}}})
				return cmd
			}
		}
	}
	cmd.SetErr(redis.Nil)
	return cmd
}

func (m *mockCmdable) XInfoGroups(ctx context.Context, stream string) *redis.XInfoGroupsCmd {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.xInfoGroupsCalls = append(m.xInfoGroupsCalls, stream)

	cmd := redis.NewXInfoGroupsCmd(ctx, stream)
	if resp, ok := m.xInfoGroupsResp[stream]; ok {
		if resp.err != nil {
			cmd.SetErr(resp.err)
		} else {
			cmd.SetVal(resp.result)
		}
	} else {
		cmd.SetErr(redis.Nil)
	}
	return cmd
}

func (m *mockCmdable) XGroupCreateMkStream(ctx context.Context, stream, group, id string) *redis.StatusCmd {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.xGroupCreateCalls = append(m.xGroupCreateCalls, xGroupCreateCall{
		stream: stream,
		group:  group,
		id:     id,
	})

	cmd := redis.NewStatusCmd(ctx)
	key := stream + ":" + group + ":" + id
	if resp, ok := m.xGroupCreateResp[key]; ok {
		if resp.err != nil {
			cmd.SetErr(resp.err)
		} else {
			cmd.SetVal(resp.result)
		}
	} else {
		cmd.SetVal("OK")
	}
	return cmd
}

func (m *mockCmdable) XAck(ctx context.Context, stream, group string, ids ...string) *redis.IntCmd {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.xAckCalls = append(m.xAckCalls, xAckCall{
		stream: stream,
		group:  group,
		ids:    ids,
	})

	cmd := redis.NewIntCmd(ctx)
	cmd.SetVal(int64(len(ids)))
	return cmd
}

func (m *mockCmdable) XPendingExt(ctx context.Context, a *redis.XPendingExtArgs) *redis.XPendingExtCmd {
	m.mu.Lock()
	defer m.mu.Unlock()

	cmd := redis.NewXPendingExtCmd(ctx)
	key := a.Stream + ":" + a.Group
	if resp, ok := m.xPendingExtResp[key]; ok {
		if resp.err != nil {
			cmd.SetErr(resp.err)
		} else {
			cmd.SetVal(resp.result)
		}
	} else {
		cmd.SetVal([]redis.XPendingExt{})
	}
	return cmd
}

func (m *mockCmdable) XClaim(ctx context.Context, a *redis.XClaimArgs) *redis.XMessageSliceCmd {
	m.mu.Lock()
	defer m.mu.Unlock()

	cmd := redis.NewXMessageSliceCmd(ctx)
	key := a.Stream + ":" + a.Group
	if resp, ok := m.xClaimResp[key]; ok {
		if resp.err != nil {
			cmd.SetErr(resp.err)
		} else {
			cmd.SetVal(resp.result)
		}
	} else {
		cmd.SetVal([]redis.XMessage{})
	}
	return cmd
}

func (m *mockCmdable) verifyXAddCalled(t *testing.T, stream string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, call := range m.xAddCalls {
		if call.stream == stream {
			return
		}
	}
	t.Errorf("XAdd not called for stream %q", stream)
}

func (m *mockCmdable) verifyXInfoGroupsCalled(t *testing.T, stream string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, call := range m.xInfoGroupsCalls {
		if call == stream {
			return
		}
	}
	t.Errorf("XInfoGroups not called for stream %q", stream)
}

func (m *mockCmdable) verifyXGroupCreateCalled(t *testing.T, stream string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, call := range m.xGroupCreateCalls {
		if call.stream == stream {
			return
		}
	}
	t.Errorf("XGroupCreateMkStream not called for stream %q", stream)
}

func (m *mockCmdable) verifyXAckCalled(t *testing.T, stream, id string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, call := range m.xAckCalls {
		if call.stream == stream {
			for _, callID := range call.ids {
				if callID == id {
					return
				}
			}
		}
	}
	t.Errorf("XAck not called for stream %q, id %q", stream, id)
}

func (m *mockCmdable) verifyXAckNotCalled(t *testing.T) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if len(m.xAckCalls) > 0 {
		t.Errorf("XAck should not be called, but was called %d times", len(m.xAckCalls))
	}
}

func (m *mockCmdable) expectXPendingExt(stream, group string, result []redis.XPendingExt, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.xPendingExtResp == nil {
		m.xPendingExtResp = make(map[string]xPendingExtResponse)
	}
	key := stream + ":" + group
	m.xPendingExtResp[key] = xPendingExtResponse{result: result, err: err}
}

func (m *mockCmdable) expectXClaim(stream, group string, result []redis.XMessage, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.xClaimResp == nil {
		m.xClaimResp = make(map[string]xClaimResponse)
	}
	key := stream + ":" + group
	m.xClaimResp[key] = xClaimResponse{result: result, err: err}
}

func TestNew_WithDefaultOptions(t *testing.T) {
	t.Parallel()

	client := newMockCmdable()
	b := New(client)

	br, ok := b.(*broker)
	if !ok {
		t.Fatal("expected *broker type")
	}

	if br.client != client {
		t.Error("client not set correctly")
	}

	if br.opts.streamKeyFormat != "stream:%s" {
		t.Errorf("expected default streamKeyFormat 'stream:%%s', got %q", br.opts.streamKeyFormat)
	}
	if br.opts.consumerGroup != "consumers" {
		t.Errorf("expected default consumerGroup 'consumers', got %q", br.opts.consumerGroup)
	}
	if br.opts.processingTimeout != 30*time.Second {
		t.Errorf("expected default processingTimeout 30s, got %v", br.opts.processingTimeout)
	}
	if br.opts.claimInterval != 1*time.Second {
		t.Errorf("expected default claimInterval 1s, got %v", br.opts.claimInterval)
	}
	if br.opts.maxClaimBatch != 10 {
		t.Errorf("expected default maxClaimBatch 10, got %d", br.opts.maxClaimBatch)
	}
	if br.opts.blockTimeout != 500*time.Millisecond {
		t.Errorf("expected default blockTimeout 500ms, got %v", br.opts.blockTimeout)
	}
	if br.opts.maxStreamLength != 0 {
		t.Errorf("expected default maxStreamLength 0, got %d", br.opts.maxStreamLength)
	}
	if !br.opts.approximateTrim {
		t.Error("expected default approximateTrim true")
	}
	if !br.opts.enableClaim {
		t.Error("expected default enableClaim true")
	}
	if br.opts.consumerPrefix != "" {
		t.Errorf("expected default consumerPrefix empty, got %q", br.opts.consumerPrefix)
	}
}

func TestNew_WithCustomOptions(t *testing.T) {
	t.Parallel()

	client := newMockCmdable()
	b := New(client,
		WithStreamKeyFormat("custom:%s"),
		WithConsumerGroup("mygroup"),
		WithProcessingTimeout(60*time.Second),
		WithClaimInterval(5*time.Second),
		WithMaxClaimBatch(20),
		WithBlockTimeout(1*time.Second),
		WithMaxStreamLength(1000),
		WithApproximateTrimming(false),
		WithClaim(false),
		WithConsumerPrefix("test"),
	)

	br := b.(*broker)

	if br.opts.streamKeyFormat != "custom:%s" {
		t.Errorf("expected streamKeyFormat 'custom:%%s', got %q", br.opts.streamKeyFormat)
	}
	if br.opts.consumerGroup != "mygroup" {
		t.Errorf("expected consumerGroup 'mygroup', got %q", br.opts.consumerGroup)
	}
	if br.opts.processingTimeout != 60*time.Second {
		t.Errorf("expected processingTimeout 60s, got %v", br.opts.processingTimeout)
	}
	if br.opts.claimInterval != 5*time.Second {
		t.Errorf("expected claimInterval 5s, got %v", br.opts.claimInterval)
	}
	if br.opts.maxClaimBatch != 20 {
		t.Errorf("expected maxClaimBatch 20, got %d", br.opts.maxClaimBatch)
	}
	if br.opts.blockTimeout != 1*time.Second {
		t.Errorf("expected blockTimeout 1s, got %v", br.opts.blockTimeout)
	}
	if br.opts.maxStreamLength != 1000 {
		t.Errorf("expected maxStreamLength 1000, got %d", br.opts.maxStreamLength)
	}
	if br.opts.approximateTrim {
		t.Error("expected approximateTrim false")
	}
	if br.opts.enableClaim {
		t.Error("expected enableClaim false")
	}
	if br.opts.consumerPrefix != "test" {
		t.Errorf("expected consumerPrefix 'test', got %q", br.opts.consumerPrefix)
	}
}

func TestBroker_Consume_GroupExistsError(t *testing.T) {
	mock := newMockCmdable()
	b := &broker{
		client: mock,
		opts: &options{
			streamKeyFormat: "stream:%s",
			consumerGroup:   "consumers",
		},
		consumers: make(map[string][]context.CancelFunc),
	}

	expectedErr := errors.New("redis connection error")
	mock.expectXInfoGroups("stream:test", nil, expectedErr)

	ctx := context.Background()
	err := b.Consume(ctx, "test", func(data []byte) error {
		return nil
	})

	if err == nil {
		t.Error("expected error, got nil")
	}
	if !errors.Is(err, ErrGroupCheckFailed) {
		t.Errorf("expected ErrGroupCheckFailed, got %v", err)
	}
}

func TestBroker_GroupExists_Error(t *testing.T) {
	t.Parallel()

	mock := newMockCmdable()
	b := &broker{client: mock, opts: &options{}}

	expectedErr := errors.New("connection timeout")
	mock.expectXInfoGroups("stream:test", nil, expectedErr)

	exists, err := b.groupExists(context.Background(), "stream:test", "group")
	if err == nil {
		t.Error("expected error, got nil")
	}
	if !errors.Is(err, expectedErr) {
		t.Errorf("expected connection error, got %v", err)
	}
	if exists {
		t.Error("expected exists to be false on error")
	}
}

func TestBroker_ClaimStalledMessages_Success(t *testing.T) {
	mock := newMockCmdable()
	b := &broker{
		client: mock,
		opts: &options{
			streamKeyFormat:   "stream:%s",
			consumerGroup:     "consumers",
			maxClaimBatch:     10,
			processingTimeout: 30 * time.Second,
			claimInterval:     100 * time.Millisecond,
			blockTimeout:      10 * time.Millisecond,
			enableClaim:       true,
		},
		consumers: make(map[string][]context.CancelFunc),
	}

	mock.expectXInfoGroups("stream:test", []redis.XInfoGroup{{Name: "consumers:test"}}, nil)

	stalledMsg := redis.XMessage{
		ID:     "stalled-1",
		Values: map[string]interface{}{"payload": `{"data":"c3RhbGxlZA==","enqueued_at":"2025-01-01T00:00:00Z"}`},
	}

	mock.expectXPendingExt("stream:test", "consumers:test", []redis.XPendingExt{
		{ID: "stalled-1"},
	}, nil)

	mock.expectXClaim("stream:test", "consumers:test", []redis.XMessage{stalledMsg}, nil)
	mock.expectXAck("stream:test", "consumers:test", []string{"stalled-1"}, 1, nil)
	mock.setXReadGroupAlwaysNil("stream:test", "consumers:test")

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	handlerCalled := make(chan struct{})
	go func() {
		_ = b.Consume(ctx, "test", func(data []byte) error {
			if string(data) == "stalled" {
				close(handlerCalled)
			}
			return nil
		})
	}()

	select {
	case <-handlerCalled:
	case <-time.After(500 * time.Millisecond):
		t.Error("stalled message was not claimed and processed")
	}

	cancel()
}

func TestBroker_ClaimStalledMessages_PendingError(t *testing.T) {
	mock := newMockCmdable()
	b := &broker{client: mock, opts: &options{maxClaimBatch: 10, processingTimeout: 30 * time.Second}}

	mock.expectXPendingExt("stream:test", "group", nil, errors.New("pending error"))

	b.claimStalledMessages(context.Background(), "stream:test", "group", "consumer", func([]byte) error {
		return nil
	})
}

func TestBroker_ClaimStalledMessages_InvalidPayload(t *testing.T) {
	mock := newMockCmdable()
	b := &broker{client: mock, opts: &options{maxClaimBatch: 10, processingTimeout: 30 * time.Second}}

	mock.expectXPendingExt("stream:test", "group", []redis.XPendingExt{{ID: "msg-1"}}, nil)

	invalidMsg := redis.XMessage{
		ID:     "msg-1",
		Values: map[string]interface{}{"wrong_field": "data"},
	}
	mock.expectXClaim("stream:test", "group", []redis.XMessage{invalidMsg}, nil)
	mock.expectXAck("stream:test", "group", []string{"msg-1"}, 1, nil)

	b.claimStalledMessages(context.Background(), "stream:test", "group", "consumer", func([]byte) error {
		t.Error("handler should not be called for invalid payload")
		return nil
	})

	mock.verifyXAckCalled(t, "stream:test", "msg-1")
}

func TestBroker_ClaimStalledMessages_HandlerError(t *testing.T) {
	mock := newMockCmdable()
	b := &broker{client: mock, opts: &options{maxClaimBatch: 10, processingTimeout: 30 * time.Second}}

	mock.expectXPendingExt("stream:test", "group", []redis.XPendingExt{{ID: "msg-1"}}, nil)

	msg := redis.XMessage{
		ID:     "msg-1",
		Values: map[string]interface{}{"payload": `{"data":"dGVzdA==","enqueued_at":"2025-01-01T00:00:00Z"}`},
	}
	mock.expectXClaim("stream:test", "group", []redis.XMessage{msg}, nil)

	handlerCalled := false
	b.claimStalledMessages(context.Background(), "stream:test", "group", "consumer", func(data []byte) error {
		handlerCalled = true
		return errors.New("processing error")
	})

	if !handlerCalled {
		t.Error("handler should be called")
	}

	mock.verifyXAckNotCalled(t)
}

func TestBroker_ClaimStalledMessages_ClaimError(t *testing.T) {
	mock := newMockCmdable()
	b := &broker{client: mock, opts: &options{maxClaimBatch: 10, processingTimeout: 30 * time.Second}}

	mock.expectXPendingExt("stream:test", "group", []redis.XPendingExt{{ID: "msg-1"}}, nil)
	mock.expectXClaim("stream:test", "group", nil, errors.New("claim error"))

	b.claimStalledMessages(context.Background(), "stream:test", "group", "consumer", func([]byte) error {
		t.Error("handler should not be called when claim fails")
		return nil
	})
}

func TestBroker_TrackConsumer(t *testing.T) {
	t.Parallel()

	b := &broker{
		consumers: make(map[string][]context.CancelFunc),
	}

	cancelCalled := false
	cancel := func() {
		cancelCalled = true
	}

	b.trackConsumer("topic1", cancel)

	if len(b.consumers["topic1"]) != 1 {
		t.Errorf("expected 1 consumer for topic1, got %d", len(b.consumers["topic1"]))
	}

	b.trackConsumer("topic1", cancel)

	if len(b.consumers["topic1"]) != 2 {
		t.Errorf("expected 2 consumers for topic1, got %d", len(b.consumers["topic1"]))
	}

	b.consumers["topic1"][0]()

	if !cancelCalled {
		t.Error("cancel function was not called")
	}
}
