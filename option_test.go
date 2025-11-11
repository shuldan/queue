package queue

import (
	"runtime"
	"testing"
	"time"
)

func TestWithPrefix(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		prefix string
	}{
		{"empty_prefix", ""},
		{"simple_prefix", "test:"},
		{"complex_prefix", "prod:app:v1:"},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			opts := &options{}
			WithPrefix(tt.prefix)(opts)
			if opts.prefix != tt.prefix {
				t.Errorf("expected %q, got %q", tt.prefix, opts.prefix)
			}
		})
	}
}

func TestWithDLQ(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		enabled bool
	}{
		{"dlq_enabled", true},
		{"dlq_disabled", false},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			opts := &options{}
			WithDLQ(tt.enabled)(opts)
			if opts.dlqEnabled != tt.enabled {
				t.Errorf("expected %v, got %v", tt.enabled, opts.dlqEnabled)
			}
		})
	}
}

func TestWithWorkerCount_LessThanOne(t *testing.T) {
	t.Parallel()

	tests := []int{-100, -1, 0}

	for _, n := range tests {
		n := n
		opts := &options{}
		WithWorkerCount(n)(opts)
		if opts.workerCount != 1 {
			t.Errorf("n=%d: expected 1, got %d", n, opts.workerCount)
		}
	}
}

func TestWithWorkerCount_ValidValues(t *testing.T) {
	t.Parallel()

	tests := []int{1, 2, 10, 100, runtime.NumCPU()}

	for _, n := range tests {
		n := n
		opts := &options{}
		WithWorkerCount(n)(opts)
		if opts.workerCount != n {
			t.Errorf("expected %d, got %d", n, opts.workerCount)
		}
	}
}

func TestWithMaxRetries(t *testing.T) {
	t.Parallel()

	tests := []int{-1, 0, 1, 3, 10, 100}

	for _, n := range tests {
		n := n
		opts := &options{}
		WithMaxRetries(n)(opts)
		if opts.maxRetries != n {
			t.Errorf("expected %d, got %d", n, opts.maxRetries)
		}
	}
}

func TestWithBackoff(t *testing.T) {
	t.Parallel()

	fb := FixedBackoff{Duration: 5 * time.Second}
	opts := &options{}
	WithBackoff(fb)(opts)

	if opts.backoff == nil {
		t.Error("expected backoff to be set")
	}

	if opts.backoff.Delay(0) != 5*time.Second {
		t.Errorf("expected 5s, got %v", opts.backoff.Delay(0))
	}
}

func TestWithErrorHandler(t *testing.T) {
	t.Parallel()

	handler := &defaultErrorHandler{}
	opts := &options{}
	WithErrorHandler(handler)(opts)

	if opts.errorHandler != handler {
		t.Error("expected error handler to be set")
	}
}

func TestWithPanicHandler(t *testing.T) {
	t.Parallel()

	handler := &defaultPanicHandler{}
	opts := &options{}
	WithPanicHandler(handler)(opts)

	if opts.panicHandler != handler {
		t.Error("expected panic handler to be set")
	}
}
