package tests

import (
	"context"
	"testing"
	"time"

	"github.com/iamitprakash/GMP-go/pkg/gmp"
)

func TestFutureExecution(t *testing.T) {
	s := gmp.NewScheduler(2, 64)
	s.Start()
	defer s.Stop()

	// 1. Test Successful Future
	future := gmp.SubmitFuture(s, func(ctx context.Context) (string, error) {
		time.Sleep(10 * time.Millisecond)
		return "success", nil
	})

	res, err := future.Await(context.Background())
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}
	if res != "success" {
		t.Errorf("Expected 'success', got '%s'", res)
	}
}

func TestFutureCancellation(t *testing.T) {
	s := gmp.NewScheduler(2, 64)
	s.Start()
	defer s.Stop()

	// 2. Test Context Cancellation
	future := gmp.SubmitFuture(s, func(ctx context.Context) (int, error) {
		time.Sleep(100 * time.Millisecond) // long blocking op
		return 42, nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	val, err := future.Await(ctx)
	if err == nil {
		t.Errorf("Expected cancellation error, but got nil with value: %d", val)
	}
	if err != context.DeadlineExceeded {
		t.Errorf("Expected DeadlineExceeded, got: %v", err)
	}
}
