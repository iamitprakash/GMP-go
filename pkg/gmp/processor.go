package gmp

import (
	"github.com/iamitprakash/GMP-go/pkg/queue"
)

// P represents a Processor in the GMP model.
// An M must acquire a P to execute Tasks.
type P struct {
	ID     int
	LocalQ *queue.BoundedQueue[*Task]
}

// NewP creates a new Processor with a local queue of the specified capacity.
func NewP(id int, localQCapacity int) *P {
	return &P{
		ID:     id,
		LocalQ: queue.NewBoundedQueue[*Task](localQCapacity),
	}
}
