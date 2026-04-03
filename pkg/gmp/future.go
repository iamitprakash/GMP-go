package gmp

import (
	"context"
)

// Future represents a generic handle to an asynchronous task's result.
type Future[T any] struct {
	result T
	err    error
	done   chan struct{}
}

func newFuture[T any]() *Future[T] {
	return &Future[T]{
		done: make(chan struct{}),
	}
}

// Await blocks until the asynchronous task completes or the provided context is canceled.
func (f *Future[T]) Await(ctx context.Context) (T, error) {
	select {
	case <-f.done:
		return f.result, f.err
	case <-ctx.Done():
		var empty T
		return empty, ctx.Err()
	}
}

// SubmitFuture is a generic helper that wraps a function returning (T, error) into a standard Task.
// It returns a Future[T] allowing the caller to await the completion of the execution.
func SubmitFuture[T any](s *Scheduler, fn func(context.Context) (T, error)) *Future[T] {
	f := newFuture[T]()

	s.Submit(func(ctx context.Context) {
		res, err := fn(ctx)
		f.result = res
		f.err = err
		close(f.done)
	})

	return f
}
