package redis

import "errors"

var (
	ErrBrokerClosed       = errors.New("broker is closed")
	ErrInvalidPayload     = errors.New("missing or invalid 'payload' field")
	ErrProduceFailed      = errors.New("failed to produce message to Redis stream")
	ErrEncodeFailed       = errors.New("failed to encode message for Redis")
	ErrConsumeSetupFailed = errors.New("failed to setup consumer group in Redis")
	ErrGroupCheckFailed   = errors.New("failed to check consumer group existence")
)
