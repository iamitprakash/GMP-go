package tests

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/iamitprakash/GMP-go/pkg/gmp"
)

func TestNetpoller(t *testing.T) {
	s := gmp.NewScheduler(2, 64)
	s.Start()
	defer s.Stop()

	poller := gmp.NewNetpoller(s)
	
	var wg sync.WaitGroup
	wg.Add(1)

	var result string
	poller.Await("SOCKET_READY", func(ctx context.Context) {
		result = "success"
		wg.Done()
	})

	// Simulate socket waking up asynchronously safely through OS event callback mappings
	go func() {
		time.Sleep(10 * time.Millisecond)
		poller.Trigger("SOCKET_READY")
	}()

	wg.Wait()
	
	if result != "success" {
		t.Errorf("Expected result 'success', got %s", result)
	}
}
