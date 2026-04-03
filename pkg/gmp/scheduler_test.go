package gmp

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
)

func TestSchedulerBasic(t *testing.T) {
	s := NewScheduler(4, 1024)
	s.Start()

	var counter int32
	var wg sync.WaitGroup

	numTasks := 100000
	for i := 0; i < numTasks; i++ {
		wg.Add(1)
		s.Submit(func(ctx context.Context) {
			atomic.AddInt32(&counter, 1)
			wg.Done()
		})
	}

	wg.Wait()
	s.Stop()

	if int(counter) != numTasks {
		t.Errorf("Expected %d tasks completed, got %d", numTasks, counter)
	}
}

// TestWorkStealing tests if work stealing works, by targeting
// a single P with tasks and letting other M's steal from it.
func TestWorkStealing(t *testing.T) {
	s := NewScheduler(4, 1024) // 4 Processors
	s.Start()

	var counter int32
	var wg sync.WaitGroup

	numTasks := 1000
	
	// Force all tasks into the local queue of P[0] directly (simulating unbalanced load)
	for i := 0; i < numTasks; i++ {
		wg.Add(1)
		t := &Task{
			Fn: func(ctx context.Context) {
				atomic.AddInt32(&counter, 1)
				wg.Done()
			},
		}
		// Try pushing directly to P[0], ignore errors for test brevity
		_ = s.Ps[0].LocalQ.PushBack(t)
	}

	// Wake up idle Ms manually since we bypassed s.Submit()
	s.idleCond.L.Lock()
	s.idleCond.Broadcast()
	s.idleCond.L.Unlock()

	wg.Wait()
	s.Stop()

	if int(counter) != numTasks {
		t.Errorf("Expected %d tasks completed via stealing, got %d", numTasks, counter)
	}
}
