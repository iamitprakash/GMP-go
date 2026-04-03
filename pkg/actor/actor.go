package actor

import (
	"context"
	"sync"

	"github.com/amitprakash/gmp-go/pkg/gmp"
	"github.com/amitprakash/gmp-go/pkg/queue"
)

// Actor maps a state-isolated entity onto the GMP subsystem utilizing work-stealing backbone
// to ensure highly secure multi-user message passing without exposing shared manual locking behaviors.
type Actor[T any] struct {
	Mailbox *queue.UnboundedQueue[T]
	handler func(msg T)
	sched   *gmp.Scheduler

	// Limits execution pulls exclusively to a singleton GMP routine per actor
	processing bool
	mu         sync.Mutex // Restricts boundary access exclusively over the processing loop toggle
}

// Spawn spins up a generic actor bound to a central GMP engine, tracking its own isolated message routine closures.
func Spawn[T any](s *gmp.Scheduler, handler func(msg T)) *Actor[T] {
	return &Actor[T]{
		Mailbox: queue.NewUnboundedQueue[T](),
		handler: handler,
		sched:   s,
	}
}

// Send securely loads a generic message natively onto the unbounded isolated mailbox queue
// and requests a threaded drain from the global pool.
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
			// Double check validation to prevent Race insertions during teardown
			if a.Mailbox.Len() == 0 {
				a.processing = false
				a.mu.Unlock()
				return
			}
			a.mu.Unlock()
		}
	}
}
