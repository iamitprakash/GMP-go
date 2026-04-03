package tests

import (
	"sync"
	"testing"
	"time"

	"github.com/iamitprakash/GMP-go/pkg/actor"
	"github.com/iamitprakash/GMP-go/pkg/gmp"
)

func TestActorSupervisionFaultTolerance(t *testing.T) {
	s := gmp.NewScheduler(2, 64)
	s.Start()
	defer s.Stop()

	var wg sync.WaitGroup
	wg.Add(2) // 2 Messages (one triggers panic, one resumes naturally afterwards)

	// Spin up Fault-Tolerant Actor System natively handling unmanaged Panics organically!
	act := actor.SpawnSupervised(s, func(msg string) {
		if msg == "POISON" {
			// Trigger unrecoverable Native Runtime Panic
			defer wg.Done()
			panic("Simulated memory logic boundary breach panicking active thread")
		}
		
		if msg == "NATIVE" {
			// Ensure routine restarts cleanly processing mail queues without missing data!
			defer wg.Done()
		}
	})

	// Dispatch 1 Poison thread + 1 Normal. 
	// Without Actor Supervisions, the panicking boundary will drop the active OS M-Thread instance forever!
	act.Send("POISON")
	act.Send("NATIVE")

	// Verify Fault Tolerance fully resurrects organically
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// Success!
		if act.Restarts != 1 {
			t.Errorf("Expected exactly 1 Supervisor Auto-Recovery loop triggered, saw %d", act.Restarts)
		}
	case <-time.After(500 * time.Millisecond):
		t.Errorf("Actor system froze natively. Supervisor panic recovery failed to execute closures.")
	}
}
