package queue

import "log/slog"

type ErrorContext struct {
	Topic     string
	MessageID string
	Attempt   int
	Err       error
}

type PanicContext struct {
	Topic      string
	MessageID  string
	PanicValue any
	Stack      []byte
}

type ErrorHandler interface {
	Handle(ctx ErrorContext)
}

type PanicHandler interface {
	Handle(ctx PanicContext)
}

type defaultPanicHandler struct{}

func (d *defaultPanicHandler) Handle(ctx PanicContext) {
	slog.Error("queue panic",
		"topic", ctx.Topic,
		"message_id", ctx.MessageID,
		"panic", ctx.PanicValue,
		"stack", string(ctx.Stack),
	)
}

type defaultErrorHandler struct{}

func (d *defaultErrorHandler) Handle(ctx ErrorContext) {
	slog.Error("queue error",
		"topic", ctx.Topic,
		"message_id", ctx.MessageID,
		"attempt", ctx.Attempt,
		"error", ctx.Err,
	)
}
