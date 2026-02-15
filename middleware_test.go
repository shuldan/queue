package queue

import (
	"context"
	"fmt"
	"testing"
)

func TestChainMiddleware_NoMiddleware(t *testing.T) {
	t.Parallel()
	called := false
	handler := func(_ context.Context, v int) error {
		called = true
		return nil
	}
	chained := chainMiddleware(handler, nil)
	if err := chained(context.Background(), 42); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !called {
		t.Error("handler was not called")
	}
}

func TestChainMiddleware_SingleMiddleware(t *testing.T) {
	t.Parallel()
	var order []string
	mw := func(next func(context.Context, int) error) func(context.Context, int) error {
		return func(ctx context.Context, v int) error {
			order = append(order, "before")
			err := next(ctx, v)
			order = append(order, "after")
			return err
		}
	}
	handler := func(_ context.Context, v int) error {
		order = append(order, "handler")
		return nil
	}
	chained := chainMiddleware(handler, []Middleware[int]{mw})
	_ = chained(context.Background(), 1)
	want := []string{"before", "handler", "after"}
	if len(order) != len(want) {
		t.Fatalf("expected %v, got %v", want, order)
	}
	for i := range want {
		if order[i] != want[i] {
			t.Errorf("index %d: expected %q, got %q", i, want[i], order[i])
		}
	}
}

func TestChainMiddleware_MultipleMiddlewares(t *testing.T) {
	t.Parallel()
	var order []string
	makeMW := func(name string) Middleware[int] {
		return func(next func(context.Context, int) error) func(context.Context, int) error {
			return func(ctx context.Context, v int) error {
				order = append(order, name+"-before")
				err := next(ctx, v)
				order = append(order, name+"-after")
				return err
			}
		}
	}
	handler := func(_ context.Context, _ int) error {
		order = append(order, "handler")
		return nil
	}
	mws := []Middleware[int]{makeMW("A"), makeMW("B")}
	chained := chainMiddleware(handler, mws)
	_ = chained(context.Background(), 0)
	want := []string{"A-before", "B-before", "handler", "B-after", "A-after"}
	if len(order) != len(want) {
		t.Fatalf("expected %v, got %v", want, order)
	}
	for i := range want {
		if order[i] != want[i] {
			t.Errorf("index %d: expected %q, got %q", i, want[i], order[i])
		}
	}
}

func TestChainMiddleware_ErrorPropagation(t *testing.T) {
	t.Parallel()
	handler := func(_ context.Context, _ int) error {
		return fmt.Errorf("fail")
	}
	mw := func(next func(context.Context, int) error) func(context.Context, int) error {
		return func(ctx context.Context, v int) error {
			return next(ctx, v)
		}
	}
	chained := chainMiddleware(handler, []Middleware[int]{mw})
	err := chained(context.Background(), 0)
	if err == nil || err.Error() != "fail" {
		t.Errorf("expected 'fail', got %v", err)
	}
}
