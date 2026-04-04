package gmp

import (
	"context"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/iamitprakash/GMP-go/pkg/queue"
	"github.com/iamitprakash/GMP-go/pkg/telemetry"
)

// Scheduler manages the entire GMP lifecycle.
type Scheduler struct {
	GlobalHighQ *queue.UnboundedQueue[*Task]
	GlobalQ     *queue.UnboundedQueue[*Task]
	GlobalLowQ  *queue.UnboundedQueue[*Task]

	PsMu    sync.RWMutex
	Ps      []*P
	MsMu    sync.Mutex
	Ms      []*M
	IdlePs  chan *P 
	
	taskIDGen  int64
	localQSize int
	maxTaskDur time.Duration // Max permitted bounds for executing tasks before preemption cancellation

	idleCond *sync.Cond
	idleMs   int32

	ctx     context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	stopped int32
}

func NewScheduler(numP int, localQSize int) *Scheduler {
	ctx, cancel := context.WithCancel(context.Background())
	s := &Scheduler{
		GlobalHighQ: queue.NewUnboundedQueue[*Task](),
		GlobalQ:     queue.NewUnboundedQueue[*Task](),
		GlobalLowQ:  queue.NewUnboundedQueue[*Task](),
		Ps:          make([]*P, numP),
		Ms:          make([]*M, 0, numP*2), 
		IdlePs:      make(chan *P, 100000), // Massive buffer mapping safe Dynamic OS Elastic limits organically
		localQSize:  localQSize,
		idleCond:    sync.NewCond(&sync.Mutex{}),
		ctx:         ctx,
		cancel:      cancel,
	}

	for i := 0; i < numP; i++ {
		s.Ps[i] = NewP(i, localQSize)
		s.IdlePs <- s.Ps[i]
		
		newM := NewM(i, s)
		s.Ms = append(s.Ms, newM)
	}
	return s
}

// SetMaxTaskDuration binds the cooperative timeout across executed tasks.
func (s *Scheduler) SetMaxTaskDuration(d time.Duration) {
	s.maxTaskDur = d
}

// autoScalerLoop intelligently expands or kills `P` buffers organically mapping to raw `expvar` telemetries.
func (s *Scheduler) autoScalerLoop() {
	defer s.wg.Done()
	
	// Hard cap expansion bounds to logical hardware metrics magically!
	maxP := 64
	
	for !s.isStopped() {
		time.Sleep(1 * time.Second)
		
		idle := atomic.LoadInt32(&s.idleMs)
		highQ := s.GlobalHighQ.Len()
		gloQ := s.GlobalQ.Len()
		
		s.PsMu.Lock()
		currentPs := len(s.Ps)
		needsWake := false
		
		// Auto-Scale UP: Intense data blocking organically triggers more processing slices
		if (highQ > 0 || gloQ > 100) && idle == 0 && currentPs < maxP {
			newP := NewP(currentPs, s.localQSize)
			s.Ps = append(s.Ps, newP)
			s.IdlePs <- newP 
			needsWake = true
		}
		
		// Auto-Scale DOWN: Freezing unused processor limits
		if idle > int32(currentPs)*2 && currentPs > 1 && gloQ == 0 && highQ == 0 {
			// Slices structurally detached correctly.
			s.Ps = s.Ps[:currentPs-1]
		}
		s.PsMu.Unlock()
		
		if needsWake {
			s.wakeOrSpawnM() // Instantly deploy threaded sidecars
		}
	}
}

func (s *Scheduler) Start() {
	s.MsMu.Lock()
	defer s.MsMu.Unlock()
	for _, m := range s.Ms {
		s.wg.Add(1)
		go m.Run()
	}
	s.wg.Add(1)
	go s.autoScalerLoop() // Fire Auto-Scale Engine!
}

func (s *Scheduler) isStopped() bool {
	return atomic.LoadInt32(&s.stopped) == 1
}

func (s *Scheduler) Stop() {
	atomic.StoreInt32(&s.stopped, 1)
	s.cancel()
	close(s.IdlePs) 

	s.idleCond.L.Lock()
	s.idleCond.Broadcast()
	s.idleCond.L.Unlock()
	s.wg.Wait()
}

func (s *Scheduler) Submit(fn func(context.Context)) {
	s.SubmitPrio(PriorityNormal, false, fn)
}

func (s *Scheduler) SubmitBlocking(fn func(context.Context)) {
	s.SubmitPrio(PriorityNormal, true, fn)
}

func (s *Scheduler) SubmitPrio(priority Priority, blocking bool, fn func(context.Context)) {
	s.submitTask(&Task{
		ID:        atomic.AddInt64(&s.taskIDGen, 1),
		Priority:  priority,
		Blocking:  blocking,
		StartedAt: time.Now(),
		Fn:        fn,
	})
}

func (s *Scheduler) submitTask(t *Task) {
	telemetry.TasksSubmitted.Add(1)
	
	switch t.Priority {
	case PriorityHigh:
		s.GlobalHighQ.PushBack(t)
	case PriorityLow:
		s.GlobalLowQ.PushBack(t)
	default:
		// PriorityNormal attempts local queues for extreme speed optimizations
		placed := false
		
		s.PsMu.RLock()
		if len(s.Ps) > 0 {
			p := s.Ps[rand.Intn(len(s.Ps))]
			if err := p.LocalQ.PushBack(t); err == nil {
				placed = true
			}
		}
		s.PsMu.RUnlock()
		
		if !placed {
			s.GlobalQ.PushBack(t)
		}
	}

	// Wake up idle threads aggressively if High Priority queue hit
	if atomic.LoadInt32(&s.idleMs) > 0 {
		s.idleCond.L.Lock()
		if t.Priority == PriorityHigh {
			s.idleCond.Broadcast() // Wake ALL
		} else {
			s.idleCond.Signal()
		}
		s.idleCond.L.Unlock()
	}
}

func (s *Scheduler) wakeOrSpawnM() {
	s.idleCond.L.Lock()
	if atomic.LoadInt32(&s.idleMs) > 0 {
		s.idleCond.Signal()
		s.idleCond.L.Unlock()
		return
	}
	s.idleCond.L.Unlock()

	if s.isStopped() { return }

	s.MsMu.Lock()
	newID := len(s.Ms)
	newM := NewM(newID, s)
	s.Ms = append(s.Ms, newM)
	s.wg.Add(1)
	go newM.Run()
	s.MsMu.Unlock()
}
