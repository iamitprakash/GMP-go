package tests

import (
	"context"
	"testing"

	"github.com/amitprakash/gmp-go/pkg/gmp"
)

func TestProcessorCreation(t *testing.T) {
	p := gmp.NewP(42, 64)

	if p.ID != 42 {
		t.Errorf("Expected Processor ID 42, got %d", p.ID)
	}

	// Validate local queue capacity rounding (64 is standard power of 2, so it stays 64 length logic underneath)
	if p.LocalQ == nil {
		t.Fatalf("Processor Local Queue was not initialized")
	}

	// Try an actual enqueue to local queue manually bypassing scheduler
	err := p.LocalQ.PushBack(&gmp.Task{
		ID: 1,
		Fn: func(ctx context.Context) {},
	})
	if err != nil {
		t.Errorf("Processor local queue failed PushBack: %v", err)
	}

	if p.LocalQ.Len() != 1 {
		t.Errorf("Processor length expected 1, got %d", p.LocalQ.Len())
	}
}
