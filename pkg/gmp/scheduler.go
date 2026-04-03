package gmp

import (
	"context"
	"math/rand"
	"sync"
	"sync/atomic"

	"github.com/amitprakash/gmp-go/pkg/queue"
	"github.com/amitprakash/gmp-go/pkg/telemetry"
)

// Scheduler manages the entire GMP lifecycle.
type Scheduler struct {
	GlobalQ *queue.UnboundedQueue[*Task]
	Ps      []*P
	
	MsMu    sync.Mutex
	Ms      []*M
	IdlePs  chan *P // Channel representing the pool of unused Processors (Handoff pool)
	
	taskIDGen  int64
	localQSize int

	idleCond *sync.Cond
	idleMs   int32

	ctx     context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	stopped int32
}

// NewScheduler creates a scheduler with numP processors.
func NewScheduler(numP int, localQSize int) *Scheduler {
	ctx, cancel := context.WithCancel(context.Background())
	s := &Scheduler{
		GlobalQ:    queue.NewUnboundedQueue[*Task](),
		Ps:         make([]*P, numP),
		Ms:         make([]*M, 0, numP*2), // Capacity pre-allocated for dynamic spawning
		IdlePs:     make(chan *P, numP),
		localQSize: localQSize,
		idleCond:   sync.NewCond(&sync.Mutex{}),
		ctx:        ctx,
		cancel:     cancel,
	}

	for i := 0; i < numP; i++ {
		s.Ps[i] = NewP(i, localQSize)
		s.IdlePs <- s.Ps[i]
		
		newM := NewM(i, s)
		s.Ms = append(s.Ms, newM)
	}
	return s
}

// Start launches all Ms (machines/threads).
func (s *Scheduler) Start() {
	s.MsMu.Lock()
	defer s.MsMu.Unlock()
	
	for _, m := range s.Ms {
		s.wg.Add(1)
		go m.Run()
	}
}

func (s *Scheduler) isStopped() bool {
	return atomic.LoadInt32(&s.stopped) == 1
}

// Stop gracefully shuts down the scheduler.
func (s *Scheduler) Stop() {
	atomic.StoreInt32(&s.stopped, 1)
	s.cancel()

	close(s.IdlePs) // Unblock dynamic Ps

	s.idleCond.L.Lock()
	s.idleCond.Broadcast()
	s.idleCond.L.Unlock()

	s.wg.Wait()
}

// Submit enqueues a standard function.
func (s *Scheduler) Submit(fn func(context.Context)) {
	s.submitTask(&Task{
		ID: atomic.AddInt64(&s.taskIDGen, 1),
		Fn: fn,
	})
}

// SubmitBlocking enqueues a system-blocking function, forcing a handoff.
func (s *Scheduler) SubmitBlocking(fn func(context.Context)) {
	s.submitTask(&Task{
		ID:       atomic.AddInt64(&s.taskIDGen, 1),
		Blocking: true,
		Fn:       fn,
	})
}

func (s *Scheduler) submitTask(t *Task) {
	telemetry.TasksSubmitted.Add(1)
	
	placed := false
	if len(s.Ps) > 0 {
		p := s.Ps[rand.Intn(len(s.Ps))]
		if err := p.LocalQ.PushBack(t); err == nil {
			placed = true
		}
	}

	if !placed {
		s.GlobalQ.PushBack(t)
	}

	if atomic.LoadInt32(&s.idleMs) > 0 {
		s.idleCond.L.Lock()
		s.idleCond.Signal()
		s.idleCond.L.Unlock()
	}
}

// wakeOrSpawnM handles the Handoff logic by ensuring a thread exists to process the newly idled P.
func (s *Scheduler) wakeOrSpawnM() {
	s.idleCond.L.Lock()
	if atomic.LoadInt32(&s.idleMs) > 0 {
		s.idleCond.Signal()
		s.idleCond.L.Unlock()
		return
	}
	s.idleCond.L.Unlock()

	if s.isStopped() {
		return
	}

	// Dynamic Spawning Protocol (Simulating Go OS thread expansion on lock)
	s.MsMu.Lock()
	newID := len(s.Ms)
	newM := NewM(newID, s)
	s.Ms = append(s.Ms, newM)
	s.wg.Add(1)
	go newM.Run()
	s.MsMu.Unlock()
}
