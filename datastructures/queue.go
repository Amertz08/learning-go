package datastructures

/*
Use cases for queue
- First in First Out (FIFO)
- Task scheduling
- Request buffering
- Message passing
- Breadth-first search
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

type linkedListQueueImpl[T any] struct {
	queue LinkedList[T]
}

func NewLinkedListQueue[T any]() Queue[T] {
	return &linkedListQueueImpl[T]{queue: NewLinkedList[T]()}
}

func (q *linkedListQueueImpl[T]) Len() int { return q.queue.Len() }

func (q *linkedListQueueImpl[T]) IsEmpty() bool {
	return q.queue.IsEmpty()
}

func (q *linkedListQueueImpl[T]) Enqueue(val T) {
	q.queue.PushFront(val)
}

func (q *linkedListQueueImpl[T]) Dequeue() (T, bool) {
	return q.queue.PopFront()
}

func (q *linkedListQueueImpl[T]) Peek() (T, bool) {
	var val T
	n, ok := q.queue.Front()
	if !ok {
		return val, ok
	}
	return n.Value(), ok
}
