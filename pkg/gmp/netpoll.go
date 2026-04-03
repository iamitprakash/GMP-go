package gmp

import (
	"context"
	"sync"
)

// Netpoller handles non-blocking event-driven execution simulation for tasks 
// that would otherwise hang OS threads waiting for sockets or generic data blocks.
type Netpoller struct {
	sched  *Scheduler
	mu     sync.Mutex
	events map[string][]func(context.Context)
}

// NewNetpoller securely binds an event-polling dispatcher to the existing scheduler.
func NewNetpoller(s *Scheduler) *Netpoller {
	return &Netpoller{
		sched:  s,
		events: make(map[string][]func(context.Context)),
	}
}

// Await registers an asynchronous closure to be executed when the specified event fires.
// Because it acts as a callback, the active thread yielding this is NEVER blocked.
func (np *Netpoller) Await(eventID string, fn func(ctx context.Context)) {
	np.mu.Lock()
	defer np.mu.Unlock()
	np.events[eventID] = append(np.events[eventID], fn)
}

// Trigger signals the event manually simulating Epoll/KQueue firing, 
// instantaneously resurrecting all awaited tasks mapped onto to the High-Priority Queue 
// sequence to ensure nanosecond scheduling response.
func (np *Netpoller) Trigger(eventID string) {
	np.mu.Lock()
	callbacks, exists := np.events[eventID]
	if exists {
		delete(np.events, eventID) // Flush map references post-trigger
	}
	np.mu.Unlock()

	for _, cb := range callbacks {
		// Route bound OS-wakes exclusively onto the prioritized execution lane
		np.sched.SubmitPrio(PriorityHigh, false, cb)
	}
}
