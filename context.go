package queue

import (
	"context"
	"time"
)

type metaKey struct{}

type MessageMeta struct {
	ID        string
	Topic     string
	Attempt   int
	Headers   map[string]string
	CreatedAt time.Time
}

func WithMeta(ctx context.Context, meta *MessageMeta) context.Context {
	return context.WithValue(ctx, metaKey{}, meta)
}

func MetaFromContext(ctx context.Context) (*MessageMeta, bool) {
	meta, ok := ctx.Value(metaKey{}).(*MessageMeta)
	return meta, ok
}
