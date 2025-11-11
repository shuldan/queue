package redis

import (
	"testing"
	"time"
)

func TestWithStreamKeyFormat(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		format string
	}{
		{"default_format", "stream:%s"},
		{"custom_format", "custom:%s"},
		{"prefix_format", "app:queue:%s"},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			opts := &options{}
			WithStreamKeyFormat(tt.format)(opts)
			if opts.streamKeyFormat != tt.format {
				t.Errorf("expected %q, got %q", tt.format, opts.streamKeyFormat)
			}
		})
	}
}

func TestWithConsumerGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		group string
	}{
		{"simple_group", "workers"},
		{"namespaced_group", "app:consumers"},
		{"empty_group", ""},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			opts := &options{}
			WithConsumerGroup(tt.group)(opts)
			if opts.consumerGroup != tt.group {
				t.Errorf("expected %q, got %q", tt.group, opts.consumerGroup)
			}
		})
	}
}

func TestWithProcessingTimeout(t *testing.T) {
	t.Parallel()

	tests := []time.Duration{
		10 * time.Second,
		30 * time.Second,
		1 * time.Minute,
		0,
	}

	for _, timeout := range tests {
		timeout := timeout
		opts := &options{}
		WithProcessingTimeout(timeout)(opts)
		if opts.processingTimeout != timeout {
			t.Errorf("expected %v, got %v", timeout, opts.processingTimeout)
		}
	}
}

func TestWithClaimInterval(t *testing.T) {
	t.Parallel()

	tests := []time.Duration{
		500 * time.Millisecond,
		1 * time.Second,
		5 * time.Second,
		0,
	}

	for _, interval := range tests {
		interval := interval
		opts := &options{}
		WithClaimInterval(interval)(opts)
		if opts.claimInterval != interval {
			t.Errorf("expected %v, got %v", interval, opts.claimInterval)
		}
	}
}

func TestWithMaxClaimBatch_ValidValues(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input    int
		expected int
	}{
		{1, 1},
		{10, 10},
		{100, 100},
	}

	for _, tt := range tests {
		tt := tt
		opts := &options{}
		WithMaxClaimBatch(tt.input)(opts)
		if opts.maxClaimBatch != tt.expected {
			t.Errorf("input %d: expected %d, got %d", tt.input, tt.expected, opts.maxClaimBatch)
		}
	}
}

func TestWithMaxClaimBatch_InvalidValues(t *testing.T) {
	t.Parallel()

	tests := []int{0, -1, -100}

	for _, n := range tests {
		n := n
		opts := &options{}
		WithMaxClaimBatch(n)(opts)
		if opts.maxClaimBatch != 0 {
			t.Errorf("input %d: expected 0, got %d", n, opts.maxClaimBatch)
		}
	}
}

func TestWithBlockTimeout(t *testing.T) {
	t.Parallel()

	tests := []time.Duration{
		100 * time.Millisecond,
		500 * time.Millisecond,
		1 * time.Second,
		0,
	}

	for _, timeout := range tests {
		timeout := timeout
		opts := &options{}
		WithBlockTimeout(timeout)(opts)
		if opts.blockTimeout != timeout {
			t.Errorf("expected %v, got %v", timeout, opts.blockTimeout)
		}
	}
}

func TestWithMaxStreamLength(t *testing.T) {
	t.Parallel()

	tests := []int64{0, 1000, 10000, -1}

	for _, maxLen := range tests {
		maxLen := maxLen
		opts := &options{}
		WithMaxStreamLength(maxLen)(opts)
		if opts.maxStreamLength != maxLen {
			t.Errorf("expected %d, got %d", maxLen, opts.maxStreamLength)
		}
	}
}

func TestWithApproximateTrimming(t *testing.T) {
	t.Parallel()

	tests := []bool{true, false}

	for _, enabled := range tests {
		enabled := enabled
		opts := &options{}
		WithApproximateTrimming(enabled)(opts)
		if opts.approximateTrim != enabled {
			t.Errorf("expected %v, got %v", enabled, opts.approximateTrim)
		}
	}
}

func TestWithClaim(t *testing.T) {
	t.Parallel()

	tests := []bool{true, false}

	for _, enabled := range tests {
		enabled := enabled
		opts := &options{}
		WithClaim(enabled)(opts)
		if opts.enableClaim != enabled {
			t.Errorf("expected %v, got %v", enabled, opts.enableClaim)
		}
	}
}

func TestWithConsumerPrefix(t *testing.T) {
	t.Parallel()

	tests := []string{"", "app", "prod-worker", "test123"}

	for _, prefix := range tests {
		prefix := prefix
		opts := &options{}
		WithConsumerPrefix(prefix)(opts)
		if opts.consumerPrefix != prefix {
			t.Errorf("expected %q, got %q", prefix, opts.consumerPrefix)
		}
	}
}
