package main

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/amitprakash/gmp-go/pkg/gmp"
)

func main() {
	fmt.Println("Starting Custom GMP Scheduler...")

	// Use number of logical CPUs for P count
	numP := runtime.NumCPU()
	localQSize := 256
	sched := gmp.NewScheduler(numP, localQSize)

	sched.Start()
	fmt.Printf("Scheduler started with %d Processors\n", numP)

	var wg sync.WaitGroup
	var completed int32
	numTasks := 1_000_000

	startTime := time.Now()

	for i := 0; i < numTasks; i++ {
		wg.Add(1)
		sched.Submit(func(ctx context.Context) {
			// Simulate some fast work
			atomic.AddInt32(&completed, 1)
			wg.Done()
		})
	}

	wg.Wait()
	duration := time.Since(startTime)

	sched.Stop()

	fmt.Printf("Successfully completed %d tasks in %s\n", completed, duration)
	fmt.Printf("Throughput: %.2f tasks/sec\n", float64(numTasks)/duration.Seconds())
}
