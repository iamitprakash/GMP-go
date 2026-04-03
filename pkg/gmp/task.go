package gmp

import "context"

// Task represents a unit of work (the 'G' in GMP model).
// In real Go, this is a goroutine with its own stack. In our user-space scheduler,
// it's a struct wrapping a function.
type Task struct {
	ID int64
	Fn func(ctx context.Context)
}
