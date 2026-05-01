package datastructures

/*
	- Stack implementation (LIFO)
	  Push and pop from the head in O(1).
	  No need for a `prev` pointer, so a singly linked list is sufficient and lightweight.

	- Queue implementation (FIFO)
	  With head + tail pointers, you can enqueue at the tail and dequeue from the head in O(1).
	  No need for bidirectional traversal.

	- Streaming / pipeline processing
	  When data flows in one direction and you only process forward.
	  Example: chaining transformations where each node represents a stage.

	- Graph traversal (adjacency lists)
	  Each vertex can store neighbors as a singly linked list.
	  Efficient for iterating neighbors without needing backward traversal.

	- Memory-constrained or simple dynamic collections
	  When you want minimal overhead per node (only one pointer instead of two).
	  Useful in embedded or performance-sensitive scenarios where simplicity matters.
*/
// TODO: go back to returning an actual node.
type LinkedList[T any] interface {
	// Insert
	PushFront(value T)
	PushBack(value T)
	InsertAt(index int, value T)

	// Remove
	PopFront() (value T, ok bool)
	PopBack() (value T, ok bool)
	RemoveAfter(index int) (value T, ok bool)

	// Access
	Front() (T, bool)
	Back() (T, bool)
	Get(index int) (value T, ok bool)

	// State
	Len() int
	IsEmpty() bool
	Contains(value T) bool
}

type nodeImpl[T any] struct {
	Value T
	Next  *nodeImpl[T]
}

//func NewLinkedList[T any]() LinkedList[T] {
//	return &linkedListImpl[T]{}
//}

type linkedListImpl[T any] struct {
	front *nodeImpl[T]
	back  *nodeImpl[T]
	len   int
}

func (l *linkedListImpl[T]) PushFront(val T) {
	tmp := l.front

	n := &nodeImpl[T]{Value: val}
	l.front = n
	l.front.Next = tmp

	if l.IsEmpty() {
		l.back = l.front
	}

	l.len++
}

func (l *linkedListImpl[T]) PushBack(val T) {
	// TODO: we're not actually reassigning prev.next = newBack
	l.back = &nodeImpl[T]{Value: val}

	if l.IsEmpty() {
		l.front = l.back
	}

	l.len++
}

func (l *linkedListImpl[T]) Front() (T, bool) {
	var zeroValue T
	if l.front == nil {
		return zeroValue, false
	}
	return l.front.Value, true
}

func (l *linkedListImpl[T]) Back() (T, bool) {
	var zeroValue T
	if l.back == nil {
		return zeroValue, false
	}
	return l.back.Value, true
}

func (l *linkedListImpl[T]) IsEmpty() bool {
	return l.len == 0
}

func (l *linkedListImpl[T]) Len() int {
	return l.len
}
