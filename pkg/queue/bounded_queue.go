package queue

import (
	"errors"
	"runtime"
	"sync/atomic"
)

var ErrQueueFull = errors.New("queue is full")
var ErrQueueEmpty = errors.New("queue is empty")

type slot[T any] struct {
	val T
	seq uint64
}

// BoundedQueue represents a lock-free multi-producer multi-consumer (MPMC) ring buffer. 
// Uses Dmitry Vyukov's array-based CAS queue algorithm for maximum throughput.
type BoundedQueue[T any] struct {
	buffer []slot[T]
	mask   uint64
	head   uint64
	tail   uint64
}

func nextPowerOf2(v uint64) uint64 {
	v--
	v |= v >> 1
	v |= v >> 2
	v |= v >> 4
	v |= v >> 8
	v |= v >> 16
	v |= v >> 32
	v++
	return v
}

// NewBoundedQueue creates a new lock-free queue. Capacity is rounded up to the nearest power of 2.
func NewBoundedQueue[T any](capacity int) *BoundedQueue[T] {
	cap64 := nextPowerOf2(uint64(capacity))
	q := &BoundedQueue[T]{
		buffer: make([]slot[T], cap64),
		mask:   cap64 - 1,
	}
	for i := range q.buffer {
		q.buffer[i].seq = uint64(i)
	}
	return q
}

// PushBack atomically adds an item. Returns ErrQueueFull if full.
func (q *BoundedQueue[T]) PushBack(item T) error {
	var cell *slot[T]
	var seq, tail uint64
	for {
		tail = atomic.LoadUint64(&q.tail)
		cell = &q.buffer[tail&q.mask]
		seq = atomic.LoadUint64(&cell.seq)
		dif := int64(seq) - int64(tail)

		if dif == 0 {
			if atomic.CompareAndSwapUint64(&q.tail, tail, tail+1) {
				break // claimed the cell
			}
		} else if dif < 0 {
			return ErrQueueFull // queue is full
		} else {
			runtime.Gosched() // Wait for another thread to modify
		}
	}
	cell.val = item
	atomic.StoreUint64(&cell.seq, tail+1)
	return nil
}

// PopFront atomically removes and returns an item.
func (q *BoundedQueue[T]) PopFront() (T, error) {
	var cell *slot[T]
	var seq, head uint64
	var empty T
	for {
		head = atomic.LoadUint64(&q.head)
		cell = &q.buffer[head&q.mask]
		seq = atomic.LoadUint64(&cell.seq)
		dif := int64(seq) - int64(head+1)

		if dif == 0 {
			if atomic.CompareAndSwapUint64(&q.head, head, head+1) {
				break // claimed the cell
			}
		} else if dif < 0 {
			return empty, ErrQueueEmpty
		} else {
			runtime.Gosched()
		}
	}
	item := cell.val
	cell.val = empty // free GC
	atomic.StoreUint64(&cell.seq, head+q.mask+1)
	return item, nil
}

// Len approximated length of the Lock-Free queue.
func (q *BoundedQueue[T]) Len() int {
	head := atomic.LoadUint64(&q.head)
	tail := atomic.LoadUint64(&q.tail)
	if tail > head {
		return int(tail - head)
	}
	return 0
}

// TakeHalf iteratively pops up to half the items, creating a work-stealing batch.
func (q *BoundedQueue[T]) TakeHalf() []T {
	length := q.Len()
	stealCount := length / 2
	if stealCount == 0 {
		return nil
	}

	stolen := make([]T, 0, stealCount)
	for i := 0; i < stealCount; i++ {
		if t, err := q.PopFront(); err == nil {
			stolen = append(stolen, t)
		} else {
			break // if queue ran empty during steal
		}
	}
	return stolen
}
