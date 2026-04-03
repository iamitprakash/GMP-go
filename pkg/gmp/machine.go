package gmp

import (
	"math/rand"
	"sync/atomic"
)

// M represents a Machine (OS Thread) in the GMP model.
// It executes Tasks from its attached P.
type M struct {
	ID    int
	Sched *Scheduler
	P     *P
	ticks uint64 // For checking global queue
}

// NewM creates a new Machine assigned to the given Scheduler and Processor.
func NewM(id int, s *Scheduler, p *P) *M {
	return &M{
		ID:    id,
		Sched: s,
		P:     p,
	}
}

// Run is the main event loop for the Machine, simulating an OS thread.
func (m *M) Run() {
	defer m.Sched.wg.Done()

	for {
		if m.Sched.isStopped() {
			return
		}

		t := m.findWork()
		if t != nil {
			t.Fn(m.Sched.ctx)
			m.ticks++
		} else {
			// No work found, go to sleep
			atomic.AddInt32(&m.Sched.idleMs, 1)
			m.Sched.idleCond.L.Lock()
			
			// Wait for work or shutdown signal
			for !m.Sched.isStopped() && m.Sched.GlobalQ.Len() == 0 && m.P.LocalQ.Len() == 0 {
				m.Sched.idleCond.Wait()
			}
			
			m.Sched.idleCond.L.Unlock()
			atomic.AddInt32(&m.Sched.idleMs, -1)
		}
	}
}

// findWork implements the work-stealing algorithm.
func (m *M) findWork() *Task {
	// 1. Every 61 ticks, check the global queue to prevent starvation
	if m.ticks != 0 && m.ticks%61 == 0 {
		if t, ok := m.Sched.GlobalQ.PopFront(); ok {
			return t
		}
	}

	// 2. Local Queue
	if t, err := m.P.LocalQ.PopFront(); err == nil {
		return t
	}

	// 3. Global Queue
	if t, ok := m.Sched.GlobalQ.PopFront(); ok {
		return t
	}

	// 4. Work Stealing
	for i := 0; i < len(m.Sched.Ps)*2; i++ {
		targetP := m.Sched.Ps[rand.Intn(len(m.Sched.Ps))]
		if targetP == m.P {
			continue
		}
		
		stolen := targetP.LocalQ.TakeHalf()
		if len(stolen) > 0 {
			// Return first stolen task
			first := stolen[0]
			// Enqueue the rest to local Q
			for j := 1; j < len(stolen); j++ {
				_ = m.P.LocalQ.PushBack(stolen[j])
			}
			return first
		}
	}

	return nil
}
