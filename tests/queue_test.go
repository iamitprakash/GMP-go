package tests

import (
	"testing"
	"github.com/iamitprakash/GMP-go/pkg/queue"
)

func TestBoundedQueue(t *testing.T) {
	q := queue.NewBoundedQueue[int](4) // Uses exact power of 2 

	if err := q.PushBack(1); err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if err := q.PushBack(2); err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if err := q.PushBack(3); err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if err := q.PushBack(4); err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	if err := q.PushBack(5); err != queue.ErrQueueFull {
		t.Errorf("Expected queue.ErrQueueFull, got %v", err)
	}

	if val, err := q.PopFront(); err != nil || val != 1 {
		t.Errorf("Expected 1, got %v (err: %v)", val, err)
	}

	if q.Len() != 3 {
		t.Errorf("Expected length 3, got %d", q.Len())
	}

	stolen := q.TakeHalf()
	if len(stolen) != 1 {
		t.Errorf("Expected to steal 1 element, got %d", len(stolen))
	}
	if stolen[0] != 2 {
		t.Errorf("Expected stolen element to be 2, got %v", stolen[0])
	}
	
	if q.Len() != 2 {
		t.Errorf("Expected length 2 after steal, got %v", q.Len())
	}
}

func TestUnboundedQueue(t *testing.T) {
	q := queue.NewUnboundedQueue[int]()

	q.PushBack(1)
	q.PushBack(2)
	q.PushBack(3)

	if q.Len() != 3 {
		t.Errorf("Expected length 3, got %d", q.Len())
	}

	if val, ok := q.PopFront(); !ok || val != 1 {
		t.Errorf("Expected 1, got %v", val)
	}

	batch := q.PopFrontN(2)
	if len(batch) != 2 {
		t.Errorf("Expected batch of 2, got %d", len(batch))
	}
	if batch[0] != 2 || batch[1] != 3 {
		t.Errorf("Expected [2, 3], got %v", batch)
	}

	if q.Len() != 0 {
		t.Errorf("Expected length 0, got %d", q.Len())
	}
}
