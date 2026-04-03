package gmp

import (
	"context"
	"time"
)

// Priority dictates execution lane mappings.
type Priority uint8

const (
	PriorityNormal Priority = iota
	PriorityHigh
	PriorityLow
)

// Task represents a unit of work (the 'G' in GMP model).
type Task struct {
	ID        int64
	Priority  Priority   // Maps to explicit execution lanes
	Blocking  bool       // If true, forces OS OS Thread Handoff
	StartedAt time.Time  // Diagnostics tracker
	Fn        func(ctx context.Context)
}
