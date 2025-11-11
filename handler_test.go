package queue

import (
	"errors"
	"testing"
)

func TestNewDefaultPanicHandler(t *testing.T) {
	t.Parallel()

	h := newDefaultPanicHandler()
	if h == nil {
		t.Error("expected non-nil panic handler")
	}

	_, ok := h.(*defaultPanicHandler)
	if !ok {
		t.Error("expected *defaultPanicHandler type")
	}
}

func TestDefaultPanicHandler_Handle(t *testing.T) {
	t.Parallel()

	h := &defaultPanicHandler{}

	defer func() {
		if r := recover(); r != nil {
			t.Errorf("Handle should not panic, got: %v", r)
		}
	}()

	h.Handle("job", "consumer", "panic value", []byte("stack trace"))
}

func TestDefaultPanicHandler_HandleNilValues(t *testing.T) {
	t.Parallel()

	h := &defaultPanicHandler{}

	defer func() {
		if r := recover(); r != nil {
			t.Errorf("Handle should not panic with nil values, got: %v", r)
		}
	}()

	h.Handle(nil, nil, nil, nil)
}

func TestNewDefaultErrorHandler(t *testing.T) {
	t.Parallel()

	h := newDefaultErrorHandler()
	if h == nil {
		t.Error("expected non-nil error handler")
	}

	_, ok := h.(*defaultErrorHandler)
	if !ok {
		t.Error("expected *defaultErrorHandler type")
	}
}

func TestDefaultErrorHandler_Handle(t *testing.T) {
	t.Parallel()

	h := &defaultErrorHandler{}

	defer func() {
		if r := recover(); r != nil {
			t.Errorf("Handle should not panic, got: %v", r)
		}
	}()

	h.Handle("job", "consumer", errors.New("test error"))
}

func TestDefaultErrorHandler_HandleNilValues(t *testing.T) {
	t.Parallel()

	h := &defaultErrorHandler{}

	defer func() {
		if r := recover(); r != nil {
			t.Errorf("Handle should not panic with nil values, got: %v", r)
		}
	}()

	h.Handle(nil, nil, nil)
}
