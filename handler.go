package queue

import (
	"log/slog"
)

type PanicHandler interface {
	Handle(job any, consumer any, panicValue any, stack []byte)
}

type ErrorHandler interface {
	Handle(job any, consumer any, err error)
}

type defaultPanicHandler struct{}

func newDefaultPanicHandler() PanicHandler {
	return &defaultPanicHandler{}
}

func (d *defaultPanicHandler) Handle(job any, consumer any, panicValue any, stack []byte) {
	slog.Error(
		"queue panic",
		"job", job,
		"consumer", consumer,
		"panic", panicValue,
		"stack", string(stack),
	)
}

type defaultErrorHandler struct{}

func newDefaultErrorHandler() ErrorHandler {
	return &defaultErrorHandler{}
}

func (d *defaultErrorHandler) Handle(job any, consumer any, err error) {
	slog.Error(
		"queue error: job=%v, consumer=%v, error=%v",
		"job", job,
		"consumer", consumer,
		"error", err,
	)
}
