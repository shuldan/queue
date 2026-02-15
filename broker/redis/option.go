package redis

import "time"

type Option func(*options)

type options struct {
	streamKeyFormat   string
	consumerGroup     string
	processingTimeout time.Duration
	claimInterval     time.Duration
	maxClaimBatch     int
	blockTimeout      time.Duration
	maxStreamLength   int64
	approximateTrim   bool
	enableClaim       bool
	consumerPrefix    string
}

func WithStreamKeyFormat(format string) Option {
	return func(o *options) { o.streamKeyFormat = format }
}

func WithConsumerGroup(group string) Option {
	return func(o *options) { o.consumerGroup = group }
}

func WithProcessingTimeout(timeout time.Duration) Option {
	return func(o *options) { o.processingTimeout = timeout }
}

func WithClaimInterval(interval time.Duration) Option {
	return func(o *options) { o.claimInterval = interval }
}

func WithMaxClaimBatch(n int) Option {
	return func(o *options) {
		if n > 0 {
			o.maxClaimBatch = n
		}
	}
}

func WithBlockTimeout(timeout time.Duration) Option {
	return func(o *options) { o.blockTimeout = timeout }
}

func WithMaxStreamLength(maxLen int64) Option {
	return func(o *options) { o.maxStreamLength = maxLen }
}

func WithApproximateTrimming(enabled bool) Option {
	return func(o *options) { o.approximateTrim = enabled }
}

func WithClaim(enabled bool) Option {
	return func(o *options) { o.enableClaim = enabled }
}

func WithConsumerPrefix(prefix string) Option {
	return func(o *options) { o.consumerPrefix = prefix }
}
