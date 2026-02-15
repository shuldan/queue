package redis

import (
	"context"
	"errors"
	"testing"
	"time"
)

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
		{"no_such_key", errors.New("ERR no such key"), true},
		{"other_error", errors.New("some other error"), false},
		{"contains_no_such_key", errors.New("prefix no such key suffix"), true},
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
	if len(id) == 0 {
		t.Fatal("expected non-empty consumer ID")
	}
	if id[:len("consumer-svc-")] != "consumer-svc-" {
		t.Errorf("expected prefix 'consumer-svc-', got %q", id)
	}
}

func TestBroker_NewConsumerID_NoPrefix(t *testing.T) {
	t.Parallel()
	b := &broker{opts: &options{consumerPrefix: ""}}
	id := b.newConsumerID("topic1")
	if len(id) == 0 {
		t.Fatal("expected non-empty consumer ID")
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
	err := b.checkClosed()
	if err == nil {
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
		streamKeyFormat: "s:%s",
		consumerGroup:   "grp",
		consumerPrefix:  "",
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

func TestHandleReadError_NormalError(t *testing.T) {
	t.Parallel()
	b := &broker{opts: &options{}}
	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
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
