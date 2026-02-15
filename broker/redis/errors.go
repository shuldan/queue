package redis

import "errors"

var (
	ErrInvalidPayload     = errors.New("missing or invalid 'payload' field")
	ErrProduceFailed      = errors.New("failed to produce message to Redis stream")
	ErrConsumeSetupFailed = errors.New("failed to setup consumer group")
	ErrGroupCheckFailed   = errors.New("failed to check consumer group existence")
)
