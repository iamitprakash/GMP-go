package gmp

import (
	"context"
	"math/rand"
	"sync/atomic"

	"github.com/iamitprakash/GMP-go/pkg/telemetry"
)

type M struct {
	ID    int
	Sched *Scheduler
	P     *P 
	ticks uint64
}

func NewM(id int, s *Scheduler) *M {
	return &M{ID: id, Sched: s}
}

func (m *M) executeTask(t *Task) {
	if m.Sched.maxTaskDur > 0 {
		// Enforce Preemption
		ctx, cancel := context.WithTimeout(m.Sched.ctx, m.Sched.maxTaskDur)
		defer cancel()
		t.Fn(ctx)
	} else {
		t.Fn(m.Sched.ctx)
	}
	m.ticks++
}

func (m *M) Run() {
	defer m.Sched.wg.Done()
	telemetry.ActiveMs.Add(1)
	defer telemetry.ActiveMs.Add(-1)

	p, ok := <-m.Sched.IdlePs
	if !ok || p == nil { return }
	m.P = p

	for !m.Sched.isStopped() {
		t := m.findWork()
		
		if t != nil {
			telemetry.TasksExecuted.Add(1)

			if t.Blocking {
				telemetry.Handoffs.Add(1)
				m.Sched.IdlePs <- m.P
				m.P = nil
				m.Sched.wakeOrSpawnM()
				
				m.executeTask(t)
				
				newP, ok := <-m.Sched.IdlePs
				if !ok || newP == nil { return }
				m.P = newP

			} else {
				m.executeTask(t)
			}
		} else {
			telemetry.IdleMs.Add(1)
			atomic.AddInt32(&m.Sched.idleMs, 1)
			m.Sched.idleCond.L.Lock()
			
			// Checks if ALL Queues are empty before properly sleeping
			for !m.Sched.isStopped() && !m.hasWorkAcrossAllQueues() {
				m.Sched.idleCond.Wait()
			}
			
			m.Sched.idleCond.L.Unlock()
			atomic.AddInt32(&m.Sched.idleMs, -1)
		}
	}
}

func (m *M) findWork() *Task {
	// 1. High Priority Execution Override
	if t, ok := m.Sched.GlobalHighQ.PopFront(); ok {
		return t
	}

	// 2. Anti-Starvation standard Global polling
	if m.ticks != 0 && m.ticks%61 == 0 {
		if t, ok := m.Sched.GlobalQ.PopFront(); ok {
			return t
		}
	}

	// 3. Local P execution queue
	if t, err := m.P.LocalQ.PopFront(); err == nil {
		return t
	}

	// 4. Global standard queue lookup
	if t, ok := m.Sched.GlobalQ.PopFront(); ok {
		return t
	}

	// 5. Work Stealing sequence securely mapping Dynamic Runtime Processor resizing loops
	m.Sched.PsMu.RLock()
	numP := len(m.Sched.Ps)
	if numP > 0 {
		startIndex := rand.Intn(numP)
		for i := 0; i < numP; i++ {
			targetP := m.Sched.Ps[(startIndex+i)%numP]
			if targetP == m.P { continue }
			
			stolen := targetP.LocalQ.TakeHalf()
			if len(stolen) > 0 {
				telemetry.TasksStolen.Add(int64(len(stolen)))
				first := stolen[0]
				for j := 1; j < len(stolen); j++ {
					if err := m.P.LocalQ.PushBack(stolen[j]); err != nil {
						m.Sched.GlobalQ.PushBack(stolen[j])
					}
				}
				m.Sched.PsMu.RUnlock()
				return first
			}
		}
	}
	m.Sched.PsMu.RUnlock()
	
	// 6. Background Activity Execution Fallback (Low priority executed only on total idle)
	if t, ok := m.Sched.GlobalLowQ.PopFront(); ok {
		return t
	}

	return nil
}

func (m *M) hasWorkAcrossAllQueues() bool {
	if m.Sched.GlobalHighQ.Len() > 0 || m.Sched.GlobalQ.Len() > 0 || m.Sched.GlobalLowQ.Len() > 0 {
		return true
	}
	m.Sched.PsMu.RLock()
	defer m.Sched.PsMu.RUnlock()
	for _, p := range m.Sched.Ps {
		if p != nil && p.LocalQ.Len() > 0 {
			return true
		}
	}
	return false
}
