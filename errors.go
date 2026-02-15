package queue

import "errors"

var (
	ErrQueueClosed     = errors.New("cannot use closed queue")
	ErrBrokerClosed    = errors.New("broker is closed")
	ErrInvalidJobType  = errors.New("job type must be a non-nil pointer to struct")
	ErrMarshal         = errors.New("failed to marshal message")
	ErrUnmarshal       = errors.New("failed to unmarshal message")
	ErrSendToDLQ       = errors.New("failed to send job to DLQ")
	ErrNilBackoff      = errors.New("backoff strategy cannot be nil")
	ErrNilErrorHandler = errors.New("error handler cannot be nil")
	ErrNilPanicHandler = errors.New("panic handler cannot be nil")
	ErrNilSerializer   = errors.New("serializer cannot be nil")
	ErrNilHandler      = errors.New("handler cannot be nil")
)
