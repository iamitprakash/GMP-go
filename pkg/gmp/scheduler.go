package gmp

import (
	"context"
	"math/rand"
	"sync"
	"sync/atomic"

	"github.com/amitprakash/gmp-go/pkg/queue"
)

// Scheduler manages the entire GMP lifecycle.
type Scheduler struct {
	GlobalQ    *queue.UnboundedQueue[*Task]
	Ps         []*P
	Ms         []*M
	
	taskIDGen  int64
	localQSize int

	idleCond *sync.Cond
	idleMs   int32

	ctx     context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	stopped int32
}

// NewScheduler creates a scheduler with numP processors and localQSize capacity for each local P queue.
func NewScheduler(numP int, localQSize int) *Scheduler {
	ctx, cancel := context.WithCancel(context.Background())
	s := &Scheduler{
		GlobalQ:    queue.NewUnboundedQueue[*Task](),
		Ps:         make([]*P, numP),
		Ms:         make([]*M, numP),
		localQSize: localQSize,
		idleCond:   sync.NewCond(&sync.Mutex{}),
		ctx:        ctx,
		cancel:     cancel,
	}

	for i := 0; i < numP; i++ {
		s.Ps[i] = NewP(i, localQSize)
		// Default tie M to P at startup
		s.Ms[i] = NewM(i, s, s.Ps[i])
	}
	return s
}

// Start launches all Ms (machines/threads).
func (s *Scheduler) Start() {
	for _, m := range s.Ms {
		s.wg.Add(1)
		go m.Run()
	}
}

// isStopped is a helper to check if scheduler is shut down.
func (s *Scheduler) isStopped() bool {
	return atomic.LoadInt32(&s.stopped) == 1
}

// Stop gracefully shuts down the scheduler.
func (s *Scheduler) Stop() {
	atomic.StoreInt32(&s.stopped, 1)
	s.cancel()

	// Wake all idle Ms so they can exit.
	s.idleCond.L.Lock()
	s.idleCond.Broadcast()
	s.idleCond.L.Unlock()

	s.wg.Wait()
}

// Submit enqueues a new function to be scheduled.
func (s *Scheduler) Submit(fn func(context.Context)) {
	t := &Task{
		ID: atomic.AddInt64(&s.taskIDGen, 1),
		Fn: fn,
	}

	placed := false
	if len(s.Ps) > 0 {
		// Attempt to place in a random P's local queue first
		p := s.Ps[rand.Intn(len(s.Ps))]
		if err := p.LocalQ.PushBack(t); err == nil {
			placed = true
		}
	}

	if !placed {
		s.GlobalQ.PushBack(t)
	}

	// Wake an idle M if needed
	if atomic.LoadInt32(&s.idleMs) > 0 {
		s.idleCond.L.Lock()
		s.idleCond.Signal()
		s.idleCond.L.Unlock()
	}
}
