package queue

import (
	"errors"
	"testing"
	"time"
)

func applyQueueOpts(opts ...Option) *options {
	o := defaultOptions()
	for _, opt := range opts {
		opt(o)
	}
	return o
}

func TestDefaultOptions_Values(t *testing.T) {
	t.Parallel()
	o := defaultOptions()
	if o.workerCount < 1 {
		t.Errorf("expected workerCount >= 1, got %d", o.workerCount)
	}
	if o.maxRetries != 3 {
		t.Errorf("expected maxRetries 3, got %d", o.maxRetries)
	}
	if o.bufferSize != 100 {
		t.Errorf("expected bufferSize 100, got %d", o.bufferSize)
	}
	if o.backoff == nil {
		t.Error("expected non-nil backoff")
	}
	if o.serializer == nil {
		t.Error("expected non-nil serializer")
	}
	if o.panicHandler == nil {
		t.Error("expected non-nil panicHandler")
	}
	if o.errorHandler == nil {
		t.Error("expected non-nil errorHandler")
	}
}

func TestWithTopic(t *testing.T) {
	t.Parallel()
	o := applyQueueOpts(WithTopic("custom"))
	if o.topic != "custom" {
		t.Errorf("expected 'custom', got %q", o.topic)
	}
}

func TestWithPrefix(t *testing.T) {
	t.Parallel()
	o := applyQueueOpts(WithPrefix("pfx:"))
	if o.prefix != "pfx:" {
		t.Errorf("expected 'pfx:', got %q", o.prefix)
	}
}

func TestWithDLQ(t *testing.T) {
	t.Parallel()
	o := applyQueueOpts(WithDLQ(true))
	if !o.dlqEnabled {
		t.Error("expected dlqEnabled true")
	}
}

func TestWithWorkerCount_Boundaries(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		n    int
		want int
	}{
		{"positive", 4, 4},
		{"zero_clamps_to_1", 0, 1},
		{"negative_clamps_to_1", -5, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			o := applyQueueOpts(WithWorkerCount(tc.n))
			if o.workerCount != tc.want {
				t.Errorf("expected %d, got %d", tc.want, o.workerCount)
			}
		})
	}
}

func TestWithMaxRetries_Boundaries(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		n    int
		want int
	}{
		{"positive", 5, 5},
		{"zero", 0, 0},
		{"negative_clamps_to_0", -3, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			o := applyQueueOpts(WithMaxRetries(tc.n))
			if o.maxRetries != tc.want {
				t.Errorf("expected %d, got %d", tc.want, o.maxRetries)
			}
		})
	}
}

func TestWithBackoff(t *testing.T) {
	t.Parallel()
	fb := FixedBackoff{Duration: 2 * time.Second}
	o := applyQueueOpts(WithBackoff(fb))
	if o.backoff != fb {
		t.Error("backoff not set correctly")
	}
}

func TestWithErrorHandler(t *testing.T) {
	t.Parallel()
	o := applyQueueOpts(WithErrorHandler(&defaultErrorHandler{}))
	if o.errorHandler == nil {
		t.Error("expected non-nil errorHandler")
	}
}

func TestWithPanicHandler(t *testing.T) {
	t.Parallel()
	o := applyQueueOpts(WithPanicHandler(&defaultPanicHandler{}))
	if o.panicHandler == nil {
		t.Error("expected non-nil panicHandler")
	}
}

func TestWithBufferSize_Boundaries(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		n    int
		want int
	}{
		{"positive", 50, 50},
		{"zero_clamps_to_1", 0, 1},
		{"negative_clamps_to_1", -10, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			o := applyQueueOpts(WithBufferSize(tc.n))
			if o.bufferSize != tc.want {
				t.Errorf("expected %d, got %d", tc.want, o.bufferSize)
			}
		})
	}
}

func TestWithSerializer(t *testing.T) {
	t.Parallel()
	o := applyQueueOpts(WithSerializer(JSONSerializer{}))
	if o.serializer == nil {
		t.Error("expected non-nil serializer")
	}
}

func TestWithHeaders(t *testing.T) {
	t.Parallel()
	h := map[string]string{"key": "val"}
	po := &produceOptions{}
	opt := WithHeaders(h)
	opt(po)
	if po.headers["key"] != "val" {
		t.Errorf("expected header key=val, got %v", po.headers)
	}
}

func TestValidateOptions_AllValid(t *testing.T) {
	t.Parallel()
	o := defaultOptions()
	if err := validateOptions(o); err != nil {
		t.Errorf("expected nil, got %v", err)
	}
}

func TestValidateOptions_NilBackoff(t *testing.T) {
	t.Parallel()
	o := defaultOptions()
	o.backoff = nil
	if err := validateOptions(o); !errors.Is(err, ErrNilBackoff) {
		t.Errorf("expected ErrNilBackoff, got %v", err)
	}
}

func TestValidateOptions_NilErrorHandler(t *testing.T) {
	t.Parallel()
	o := defaultOptions()
	o.errorHandler = nil
	if err := validateOptions(o); !errors.Is(err, ErrNilErrorHandler) {
		t.Errorf("expected ErrNilErrorHandler, got %v", err)
	}
}

func TestValidateOptions_NilPanicHandler(t *testing.T) {
	t.Parallel()
	o := defaultOptions()
	o.panicHandler = nil
	if err := validateOptions(o); !errors.Is(err, ErrNilPanicHandler) {
		t.Errorf("expected ErrNilPanicHandler, got %v", err)
	}
}

func TestValidateOptions_NilSerializer(t *testing.T) {
	t.Parallel()
	o := defaultOptions()
	o.serializer = nil
	if err := validateOptions(o); !errors.Is(err, ErrNilSerializer) {
		t.Errorf("expected ErrNilSerializer, got %v", err)
	}
}
