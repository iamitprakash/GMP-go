package main

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"time"

	"github.com/amitprakash/gmp-go/pkg/gmp"
	"github.com/amitprakash/gmp-go/pkg/telemetry"
)

func main() {
	fmt.Println("Starting Next-Gen Custom GMP Scheduler with Telemetry and Lock-Free Queues...")

	// 1. Start Telemetry Server
	telemetry.StartServer(":8080")
	fmt.Println("Telemetry server running on http://localhost:8080/debug/vars")

	numP := runtime.NumCPU()
	localQSize := 1024 // Note: bounded lock-free ring buffer rounds to power of 2 automatically
	sched := gmp.NewScheduler(numP, localQSize)

	sched.Start()
	fmt.Printf("Scheduler started with %d Processors\n", numP)

	var wg sync.WaitGroup
	numTasks := 2_000_000

	startTime := time.Now()

	// 2. Heavy Load execution
	for i := 0; i < numTasks; i++ {
		wg.Add(1)
		sched.Submit(func(ctx context.Context) {
			wg.Done()
		})
	}

	// 3. Syscall Handoff execution
	fmt.Printf("Dispatching 50 Blocking Tasks causing OS Thread Handoffs...\n")
	for i := 0; i < 50; i++ {
		wg.Add(1)
		sched.SubmitBlocking(func(ctx context.Context) {
			time.Sleep(50 * time.Millisecond) // Hard sleep simulating heavy I/O
			wg.Done()
		})
	}

	// 4. Future Await execution
	future := gmp.SubmitFuture(sched, func(ctx context.Context) (string, error) {
		time.Sleep(10 * time.Millisecond)
		return "Future Completed Successfully", nil
	})

	res, _ := future.Await(context.Background())
	fmt.Println("==> Future Result:", res)

	wg.Wait()
	duration := time.Since(startTime)
	sched.Stop()

	fmt.Printf("\n--- Execution Finished ---\n")
	fmt.Printf("Successfully completed %d regular + 51 complex tasks in %s\n", numTasks, duration)
	fmt.Printf("System Throughput: %.2f tasks/sec\n", float64(numTasks)/duration.Seconds())
	
	fmt.Printf("\n--- Final Telemetry ---\n")
	fmt.Printf("Tasks Submitted:   %s\n", telemetry.TasksSubmitted.String())
	fmt.Printf("Tasks Executed:    %s\n", telemetry.TasksExecuted.String())
	fmt.Printf("Tasks Stolen:      %s\n", telemetry.TasksStolen.String())
	fmt.Printf("Syscall Handoffs:  %s\n", telemetry.Handoffs.String())
}
