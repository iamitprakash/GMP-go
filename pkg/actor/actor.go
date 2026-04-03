package actor

import (
	"context"
	"sync"
	"log"

	"github.com/amitprakash/gmp-go/pkg/gmp"
	"github.com/amitprakash/gmp-go/pkg/queue"
)

// Actor maps a state-isolated entity onto the GMP subsystem utilizing work-stealing backbone.
type Actor[T any] struct {
	Mailbox *queue.UnboundedQueue[T]
	handler func(msg T)
	sched   *gmp.Scheduler

	// Fault Tolerance properties
	Supervised bool
	Restarts   int

	processing bool
	mu         sync.Mutex // Restricts boundary access exclusively over the processing loop toggle
}

// Spawn spins up a generic actor bound to a central GMP engine.
func Spawn[T any](s *gmp.Scheduler, handler func(msg T)) *Actor[T] {
	return &Actor[T]{
		Mailbox: queue.NewUnboundedQueue[T](),
		handler: handler,
		sched:   s,
	}
}

// SpawnSupervised attaches a Fault Tolerance Supervisor to the Actor. 
// If it panics structurally, the OS-Thread isn't killed, but rather the Actor restarts itself.
func SpawnSupervised[T any](s *gmp.Scheduler, handler func(msg T)) *Actor[T] {
	act := Spawn(s, handler)
	act.Supervised = true
	return act
}

// Send securely loads a generic message natively onto the isolated mailbox queue.
func (a *Actor[T]) Send(msg T) {
	a.Mailbox.PushBack(msg)
	a.schedule()
}

func (a *Actor[T]) schedule() {
	a.mu.Lock()
	defer a.mu.Unlock()

	if !a.processing {
		a.processing = true
		a.sched.Submit(a.processLoop)
	}
}

func (a *Actor[T]) processLoop(ctx context.Context) {
	// Erlang-Style One-For-One Supervision Recovery Hook
	defer func() {
		if r := recover(); r != nil {
			if a.Supervised {
				a.mu.Lock()
				a.Restarts++
				a.processing = false
				a.mu.Unlock()
				// Re-schedule immediately to consume remaining Mailbox independently
				a.schedule()
			} else {
				log.Printf("CRITICAL: Actor Panic (Unsupervised, shutting down): %v\n", r)
				a.mu.Lock()
				a.processing = false
				a.mu.Unlock()
			}
		}
	}()

	for {
		// Stop safely if OS preempted Context bounds internally across looping functions
		select {
		case <-ctx.Done():
			a.mu.Lock()
			a.processing = false
			a.mu.Unlock()
			return
		default:
		}

		if msg, ok := a.Mailbox.PopFront(); ok {
			a.handler(msg)
		} else {
			a.mu.Lock()
			if a.Mailbox.Len() == 0 {
				a.processing = false
				a.mu.Unlock()
				return
			}
			a.mu.Unlock()
		}
	}
}
