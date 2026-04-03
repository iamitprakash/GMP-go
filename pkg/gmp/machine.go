package gmp

import (
	"math/rand"
	"sync/atomic"

	"github.com/amitprakash/gmp-go/pkg/telemetry"
)

// M represents a Machine (OS Thread) in the GMP model.
type M struct {
	ID    int
	Sched *Scheduler
	P     *P // Dynamically assigned processor
	ticks uint64
}

// NewM creates a new Machine unassigned.
func NewM(id int, s *Scheduler) *M {
	return &M{
		ID:    id,
		Sched: s,
	}
}

// Run is the OS thread simulation.
func (m *M) Run() {
	defer m.Sched.wg.Done()
	telemetry.ActiveMs.Add(1)
	defer telemetry.ActiveMs.Add(-1)

	p, ok := <-m.Sched.IdlePs
	if !ok || p == nil {
		return 
	}
	m.P = p

	for {
		if m.Sched.isStopped() {
			return
		}

		t := m.findWork()
		if t != nil {
			telemetry.TasksExecuted.Add(1)

			if t.Blocking {
				telemetry.Handoffs.Add(1)
				
				// 1. Detach P 
				m.Sched.IdlePs <- m.P
				m.P = nil
				
				// 2. Ensure system wakes a sleeper or spawns replacement
				m.Sched.wakeOrSpawnM()
				
				// 3. Process thick task independently
				t.Fn(m.Sched.ctx)
				m.ticks++
				
				// 4. Reacquire a P after dropping out of Syscall
				newP, ok := <-m.Sched.IdlePs
				if !ok || newP == nil {
					return
				}
				m.P = newP

			} else {
				t.Fn(m.Sched.ctx)
				m.ticks++
			}
		} else {
			atomic.AddInt32(&m.Sched.idleMs, 1)
			m.Sched.idleCond.L.Lock()
			
			for !m.Sched.isStopped() && m.Sched.GlobalQ.Len() == 0 && m.P.LocalQ.Len() == 0 {
				m.Sched.idleCond.Wait()
			}
			
			m.Sched.idleCond.L.Unlock()
			atomic.AddInt32(&m.Sched.idleMs, -1)
		}
	}
}

func (m *M) findWork() *Task {
	if m.ticks != 0 && m.ticks%61 == 0 {
		if t, ok := m.Sched.GlobalQ.PopFront(); ok {
			return t
		}
	}

	if t, err := m.P.LocalQ.PopFront(); err == nil {
		return t
	}

	if t, ok := m.Sched.GlobalQ.PopFront(); ok {
		return t
	}

	for i := 0; i < len(m.Sched.Ps)*2; i++ {
		targetP := m.Sched.Ps[rand.Intn(len(m.Sched.Ps))]
		if targetP == m.P {
			continue
		}
		
		stolen := targetP.LocalQ.TakeHalf()
		if len(stolen) > 0 {
			telemetry.TasksStolen.Add(int64(len(stolen)))
			first := stolen[0]
			for j := 1; j < len(stolen); j++ {
				_ = m.P.LocalQ.PushBack(stolen[j])
			}
			return first
		}
	}
	return nil
}
