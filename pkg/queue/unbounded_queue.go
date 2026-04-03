package queue

import "sync"

// UnboundedQueue is a thread-safe queue with no upper limit, used for the global run queue.
type UnboundedQueue[T any] struct {
	items []T
	mu    sync.Mutex
}

// NewUnboundedQueue creates a new unbounded queue.
func NewUnboundedQueue[T any]() *UnboundedQueue[T] {
	return &UnboundedQueue[T]{
		items: make([]T, 0, 64),
	}
}

// PushBack adds an item to the end of the queue.
func (q *UnboundedQueue[T]) PushBack(item T) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.items = append(q.items, item)
}

// PopFront removes and returns an item from the front of the queue.
func (q *UnboundedQueue[T]) PopFront() (T, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	var empty T
	if len(q.items) == 0 {
		return empty, false
	}
	item := q.items[0]
	q.items[0] = empty // help GC

	// Re-slice to remove the first element
	q.items = q.items[1:]

	// Shrink capacity if it gets too large and mostly empty to prevent memory leaks
	// Example heuristic: length is less than 1/4 of capacity and capacity is large enough.
	if len(q.items) > 0 && len(q.items) < cap(q.items)/4 && cap(q.items) > 256 {
		newItems := make([]T, len(q.items))
		copy(newItems, q.items)
		q.items = newItems
	}

	return item, true
}

// Len returns the current number of items.
func (q *UnboundedQueue[T]) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.items)
}

// PopFrontN removes up to max items from the front of the queue.
// This is used for transferring multiple items from global to local queue.
func (q *UnboundedQueue[T]) PopFrontN(max int) []T {
	q.mu.Lock()
	defer q.mu.Unlock()

	if len(q.items) == 0 {
		return nil
	}

	amount := max
	if len(q.items) < max {
		amount = len(q.items)
	}

	popped := make([]T, amount)
	copy(popped, q.items[:amount])

	for i := 0; i < amount; i++ {
		var empty T
		q.items[i] = empty
	}
	q.items = q.items[amount:]

	return popped
}
