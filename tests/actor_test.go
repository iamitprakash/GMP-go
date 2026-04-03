package tests

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/iamitprakash/GMP-go/pkg/actor"
	"github.com/iamitprakash/GMP-go/pkg/gmp"
)

func TestActorSystem(t *testing.T) {
	s := gmp.NewScheduler(2, 64)
	s.Start()
	defer s.Stop()

	var counter int32
	var wg sync.WaitGroup

	target := int32(500)
	wg.Add(int(target))

	// Spawn the actor tracking our single integer value internally
	act := actor.Spawn(s, func(msg int) {
		atomic.AddInt32(&counter, 1)
		wg.Done()
	})

	// Send concurrently proving queue multiplexing
	for i := 0; i < int(target); i++ {
		go act.Send(i)
	}

	wg.Wait()

	if atomic.LoadInt32(&counter) != target {
		t.Errorf("Expected %d, got %d", target, counter)
	}
}
