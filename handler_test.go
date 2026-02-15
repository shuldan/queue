package queue

import (
	"errors"
	"testing"
)

func TestDefaultPanicHandler_Handle(t *testing.T) {
	t.Parallel()
	h := &defaultPanicHandler{}
	h.Handle(PanicContext{
		Topic:      "test-topic",
		MessageID:  "msg-1",
		PanicValue: "oops",
		Stack:      []byte("stack trace"),
	})
}

func TestDefaultErrorHandler_Handle(t *testing.T) {
	t.Parallel()
	h := &defaultErrorHandler{}
	h.Handle(ErrorContext{
		Topic:     "test-topic",
		MessageID: "msg-1",
		Attempt:   1,
		Err:       errors.New("test error"),
	})
}
