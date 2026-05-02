package datastructures

/*
Use cases for queue
- First in First Out (FIFO)
- Task scheduling
- Request buffering
- Message passing
- Breadth-first search

TODO: implement with LinkedList
*/

type Queue[T any] interface {
	// Enqueue adds a value to the back of the queue
	Enqueue(value T)

	// Dequeue removes and returns the front value
	// ok is false if the queue is empty
	Dequeue() (value T, ok bool)

	// Peek returns the front value without removing it
	// ok is false if the queue is empty
	Peek() (value T, ok bool)

	// Len returns the number of elements in the queue
	Len() int

	// IsEmpty reports whether the queue has no elements
	IsEmpty() bool
}

type sliceQueueImpl[T any] struct {
	queue []T
}

func NewSliceQueue[T any]() Queue[T] {
	return &sliceQueueImpl[T]{}
}

func (q *sliceQueueImpl[T]) Len() int {
	return len(q.queue)
}

func (q *sliceQueueImpl[T]) Enqueue(val T) {
	q.queue = append(q.queue, val)
}

func (q *sliceQueueImpl[T]) IsEmpty() bool {
	return len(q.queue) == 0
}

func (q *sliceQueueImpl[T]) Dequeue() (T, bool) {
	var val T
	if q.IsEmpty() {
		return val, false
	}
	val = q.queue[0]
	q.queue = q.queue[1:]
	return val, true
}

func (q *sliceQueueImpl[T]) Peek() (T, bool) {
	var val T
	if q.IsEmpty() {
		return val, false
	}
	return q.queue[0], true
}
