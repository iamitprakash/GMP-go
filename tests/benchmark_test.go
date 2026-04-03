package tests

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/iamitprakash/GMP-go/pkg/gmp"
)

// BenchmarkGMP measures the native Lock-Free work-stealing throughput 
// of the custom GMP OS-Thread Scheduler.
func BenchmarkGMP(b *testing.B) {
	s := gmp.NewScheduler(4, 1024)
	s.Start()
	defer s.Stop()

	var counter int32
	var wg sync.WaitGroup

	b.ResetTimer() // Isolate timer exclusively against execution speeds, bypassing Engine spin-up times
	
	for i := 0; i < b.N; i++ {
		wg.Add(1)
		s.SubmitPrio(gmp.PriorityNormal, false, func(ctx context.Context) {
			atomic.AddInt32(&counter, 1)
			wg.Done()
		})
	}
	
	wg.Wait()
}

// BenchmarkNativeGoroutines acts as the primary comparative baseline
// showcasing Standard Go's internal runtime execution speeds under identical workloads.
func BenchmarkNativeGoroutines(b *testing.B) {
	var counter int32
	var wg sync.WaitGroup

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		wg.Add(1)
		go func() {
			atomic.AddInt32(&counter, 1)
			wg.Done()
		}()
	}

	wg.Wait()
}
