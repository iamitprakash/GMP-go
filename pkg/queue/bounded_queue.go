package queue

import (
	"errors"
	"sync"
)

var ErrQueueFull = errors.New("queue is full")
var ErrQueueEmpty = errors.New("queue is empty")

// BoundedQueue is a thread-safe fixed-size queue, suitable for local run queues.
// We use generics so this package remains entirely independent of our gmp logic.
type BoundedQueue[T any] struct {
	items []T
	head  int
	tail  int
	count int
	size  int
	mu    sync.Mutex
}

// NewBoundedQueue creates a new bounded queue with the given size.
func NewBoundedQueue[T any](size int) *BoundedQueue[T] {
	return &BoundedQueue[T]{
		items: make([]T, size),
		size:  size,
	}
}

// PushBack adds an item to the end of the queue. Returns an error if full.
func (q *BoundedQueue[T]) PushBack(item T) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.count == q.size {
		return ErrQueueFull
	}
	q.items[q.tail] = item
	q.tail = (q.tail + 1) % q.size
	q.count++
	return nil
}

// PopFront removes and returns an item from the front of the queue.
func (q *BoundedQueue[T]) PopFront() (T, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	var empty T
	if q.count == 0 {
		return empty, ErrQueueEmpty
	}
	item := q.items[q.head]
	q.items[q.head] = empty // clear reference for GC
	q.head = (q.head + 1) % q.size
	q.count--
	return item, nil
}

// Len returns the current number of items.
func (q *BoundedQueue[T]) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.count
}

// TakeHalf removes up to half of the elements from this queue and returns them.
// Used for work stealing by other Processors in the GMP model.
func (q *BoundedQueue[T]) TakeHalf() []T {
	q.mu.Lock()
	defer q.mu.Unlock()

	stealCount := q.count / 2
	if stealCount == 0 {
		return nil
	}

	stolen := make([]T, 0, stealCount)
	for i := 0; i < stealCount; i++ {
		stolen = append(stolen, q.items[q.head])
		var empty T
		q.items[q.head] = empty
		q.head = (q.head + 1) % q.size
		q.count--
	}
	return stolen
}
