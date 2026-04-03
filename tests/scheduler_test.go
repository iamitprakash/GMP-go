package tests

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/iamitprakash/GMP-go/pkg/gmp"
)

func TestSchedulerBasic(t *testing.T) {
	s := gmp.NewScheduler(4, 1024)
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

func TestWorkStealing(t *testing.T) {
	s := gmp.NewScheduler(4, 1024) // 4 Processors
	s.Start()

	var counter int32
	var wg sync.WaitGroup

	numTasks := 1000
	
	// Force all tasks into the local queue of P[0] directly
	for i := 0; i < numTasks; i++ {
		wg.Add(1)
		task := &gmp.Task{
			Fn: func(ctx context.Context) {
				atomic.AddInt32(&counter, 1)
				wg.Done()
			},
		}
		_ = s.Ps[0].LocalQ.PushBack(task)
	}

	// Because of probabilistic stealing algorithms (rand.Intn picks random peers),
	// a single M waking up MIGHT miss P[0] and fall back to sleep if isolated. 
	// We run a watchdog to keep pinging the system context awake until drained.
	go func() {
		for atomic.LoadInt32(&counter) < int32(numTasks) {
			s.Submit(func(ctx context.Context) {})
			time.Sleep(5 * time.Millisecond)
		}
	}()

	wg.Wait()
	s.Stop()

	if int(counter) != numTasks {
		t.Errorf("Expected %d tasks completed via stealing, got %d", numTasks, counter)
	}
}
