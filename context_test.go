package queue

import (
	"context"
	"testing"
	"time"
)

func TestWithMeta_AndMetaFromContext_Success(t *testing.T) {
	t.Parallel()
	now := time.Now()
	meta := &MessageMeta{
		ID:        "id-1",
		Topic:     "topic-1",
		Attempt:   2,
		Headers:   map[string]string{"k": "v"},
		CreatedAt: now,
	}
	ctx := WithMeta(context.Background(), meta)
	got, ok := MetaFromContext(ctx)
	if !ok {
		t.Fatal("expected ok=true")
	}
	if got.ID != "id-1" {
		t.Errorf("expected ID 'id-1', got %q", got.ID)
	}
	if got.Topic != "topic-1" {
		t.Errorf("expected Topic 'topic-1', got %q", got.Topic)
	}
	if got.Attempt != 2 {
		t.Errorf("expected Attempt 2, got %d", got.Attempt)
	}
	if got.Headers["k"] != "v" {
		t.Errorf("expected header k=v, got %v", got.Headers)
	}
}

func TestMetaFromContext_NoMeta(t *testing.T) {
	t.Parallel()
	_, ok := MetaFromContext(context.Background())
	if ok {
		t.Error("expected ok=false for context without meta")
	}
}

func TestWithMeta_NilMeta_ReturnsTypedNil(t *testing.T) {
	t.Parallel()
	ctx := WithMeta(context.Background(), nil)
	got, ok := MetaFromContext(ctx)
	if !ok {
		t.Fatal("expected ok=true for typed nil *MessageMeta")
	}
	if got != nil {
		t.Errorf("expected nil pointer, got %v", got)
	}
}
