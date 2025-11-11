package queue

import "errors"

var (
	ErrQueueClosed    = errors.New("cannot use closed queue")
	ErrInvalidJobType = errors.New("job type must be a non-nil pointer to struct")
	ErrMarshal        = errors.New("failed to marshal job for DLQ")
	ErrSendToDLQ      = errors.New("failed to send job to DLQ")
)
