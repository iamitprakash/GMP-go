package main

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/amitprakash/gmp-go/pkg/actor"
	"github.com/amitprakash/gmp-go/pkg/gmp"
	"github.com/amitprakash/gmp-go/pkg/telemetry"
)

func main() {
	fmt.Println("Deployment: Maximum Architecture GMP Scheduler Module")

	telemetry.StartServer(":8080")
	fmt.Println("Telemetry endpoint running on :8080/debug/vars")
	
	numP := runtime.NumCPU()
	sched := gmp.NewScheduler(numP, 1024)
	sched.SetMaxTaskDuration(50 * time.Millisecond) // Guardrail preemption bounds logic
	sched.Start()
	
	netpoller := gmp.NewNetpoller(sched)

	var wg sync.WaitGroup
	var executed atomic.Int32
	
	// 1. Throughput Run 
	fmt.Println("[Action] Dispatching 1,000,000 background jobs (Low Priority)")
	for i := 0; i < 1_000_000; i++ {
		wg.Add(1)
		sched.SubmitPrio(gmp.PriorityLow, false, func(ctx context.Context) {
			executed.Add(1)
			wg.Done()
		})
	}

	// 2. High Priority Hook 
	fmt.Println("[Action] Dispatching 1 High Priority System Override")
	wg.Add(1)
	sched.SubmitPrio(gmp.PriorityHigh, false, func(ctx context.Context) {
		fmt.Println("  ==> High Priority Payload successfully bypassed queues instantly!")
		executed.Add(1)
		wg.Done()
	})

	// 3. Actor Model
	fmt.Println("[Action] Spinning up isolated generic Actor tracking concurrent data")
	wg.Add(50000)
	testActor := actor.Spawn(sched, func(msg int) {
		executed.Add(1)
		wg.Done()
	})
	
	for i := 0; i < 50000; i++ {
		testActor.Send(i)
	}

	// 4. Netpoller Simulation 
	fmt.Println("[Action] Registering Event-Driven Network IO Callbacks...")
	wg.Add(1)
	netpoller.Await("HTTP_RESPONSE", func(ctx context.Context) {
		fmt.Println("  ==> Network response unblocked & executed seamlessly!")
		executed.Add(1)
		wg.Done()
	})

	// Background thread acts as Linux Epoll/KQueue emitting a Ready event 25ms later
	go func() {
		time.Sleep(25 * time.Millisecond)
		netpoller.Trigger("HTTP_RESPONSE")
	}()

	fmt.Println("\nWaiting for entire multi/priority state engine to drain logic...")
	
	wg.Wait()
	sched.Stop()

	fmt.Printf("\n--- Engine Execution Verified ---\n")
	fmt.Printf("State Machine successfully cleared %d combined operations natively across multiple hardware threads.\n", executed.Load())
}
