package redis

import (
	"context"
	"testing"
	"time"
)

var _ context.Context = context.Background()

func applyOpts(opts ...Option) *options {
	o := &options{}
	for _, opt := range opts {
		opt(o)
	}
	return o
}

func TestWithStreamKeyFormat(t *testing.T) {
	t.Parallel()
	o := applyOpts(WithStreamKeyFormat("queue:%s"))
	if o.streamKeyFormat != "queue:%s" {
		t.Errorf("expected 'queue:%%s', got %q", o.streamKeyFormat)
	}
}

func TestWithConsumerGroup(t *testing.T) {
	t.Parallel()
	o := applyOpts(WithConsumerGroup("mygroup"))
	if o.consumerGroup != "mygroup" {
		t.Errorf("expected 'mygroup', got %q", o.consumerGroup)
	}
}

func TestWithProcessingTimeout(t *testing.T) {
	t.Parallel()
	o := applyOpts(WithProcessingTimeout(5 * time.Second))
	if o.processingTimeout != 5*time.Second {
		t.Errorf("expected 5s, got %v", o.processingTimeout)
	}
}

func TestWithClaimInterval(t *testing.T) {
	t.Parallel()
	o := applyOpts(WithClaimInterval(2 * time.Second))
	if o.claimInterval != 2*time.Second {
		t.Errorf("expected 2s, got %v", o.claimInterval)
	}
}

func TestWithMaxClaimBatch_Positive(t *testing.T) {
	t.Parallel()
	o := applyOpts(WithMaxClaimBatch(50))
	if o.maxClaimBatch != 50 {
		t.Errorf("expected 50, got %d", o.maxClaimBatch)
	}
}

func TestWithMaxClaimBatch_Zero(t *testing.T) {
	t.Parallel()
	o := applyOpts(WithMaxClaimBatch(0))
	if o.maxClaimBatch != 0 {
		t.Errorf("expected unchanged (0), got %d", o.maxClaimBatch)
	}
}

func TestWithMaxClaimBatch_Negative(t *testing.T) {
	t.Parallel()
	o := applyOpts(WithMaxClaimBatch(-5))
	if o.maxClaimBatch != 0 {
		t.Errorf("expected unchanged (0), got %d", o.maxClaimBatch)
	}
}

func TestWithBlockTimeout(t *testing.T) {
	t.Parallel()
	o := applyOpts(WithBlockTimeout(100 * time.Millisecond))
	if o.blockTimeout != 100*time.Millisecond {
		t.Errorf("expected 100ms, got %v", o.blockTimeout)
	}
}

func TestWithMaxStreamLength(t *testing.T) {
	t.Parallel()
	o := applyOpts(WithMaxStreamLength(5000))
	if o.maxStreamLength != 5000 {
		t.Errorf("expected 5000, got %d", o.maxStreamLength)
	}
}

func TestWithApproximateTrimming(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		enabled bool
	}{
		{"enabled", true},
		{"disabled", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			o := applyOpts(WithApproximateTrimming(tc.enabled))
			if o.approximateTrim != tc.enabled {
				t.Errorf("expected %v, got %v", tc.enabled, o.approximateTrim)
			}
		})
	}
}

func TestWithClaim(t *testing.T) {
	t.Parallel()
	o := applyOpts(WithClaim(false))
	if o.enableClaim {
		t.Error("expected enableClaim false")
	}
}

func TestWithConsumerPrefix(t *testing.T) {
	t.Parallel()
	o := applyOpts(WithConsumerPrefix("myservice"))
	if o.consumerPrefix != "myservice" {
		t.Errorf("expected 'myservice', got %q", o.consumerPrefix)
	}
}

func TestNew_WithOptions(t *testing.T) {
	t.Parallel()
	b := New(nil,
		WithStreamKeyFormat("q:%s"),
		WithConsumerGroup("cg"),
		WithConsumerPrefix("pfx"),
		WithClaim(false),
	)
	if b == nil {
		t.Fatal("expected non-nil broker")
	}
	rb := b.(*broker)
	if rb.opts.streamKeyFormat != "q:%s" {
		t.Errorf("expected 'q:%%s', got %q", rb.opts.streamKeyFormat)
	}
	if rb.opts.consumerGroup != "cg" {
		t.Errorf("expected 'cg', got %q", rb.opts.consumerGroup)
	}
	if rb.opts.enableClaim {
		t.Error("expected enableClaim false")
	}
}
