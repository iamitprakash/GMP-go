package gmp

import "context"

// Task represents a unit of work (the 'G' in GMP model).
type Task struct {
	ID       int64
	Blocking bool // If true, the executing M will detach its P during execution.
	Fn       func(ctx context.Context)
}
