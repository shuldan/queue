package queue

import "context"

type Middleware[T any] func(next func(context.Context, T) error) func(context.Context, T) error

func chainMiddleware[T any](
	handler func(context.Context, T) error,
	mws []Middleware[T],
) func(context.Context, T) error {
	for i := len(mws) - 1; i >= 0; i-- {
		handler = mws[i](handler)
	}

	return handler
}
