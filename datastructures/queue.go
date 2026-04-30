package datastructures

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

type queueImp[T any] struct {
	queue []T
}

func (q *queueImp[T]) Len() int {
	return len(q.queue)
}

func (q *queueImp[T]) Enqueue(val T) {
	q.queue = append(q.queue, val)
}

func (q *queueImp[T]) IsEmpty() bool {
	return len(q.queue) == 0
}
