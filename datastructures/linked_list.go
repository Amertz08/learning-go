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
type LinkedList[T any] interface {
	// Insert
	PushFront(value T)
	PushBack(value T)
	//InsertAfter(node LinkedListNode[T], value T) LinkedListNode[T]

	// Remove
	PopFront() (value T, ok bool)
	PopBack() (value T, ok bool)
	//RemoveAfter(node LinkedListNode[T]) (value T, ok bool)

	// Access
	Front() (LinkedListNode[T], bool)
	Back() (LinkedListNode[T], bool)
	Next(node LinkedListNode[T]) (LinkedListNode[T], bool)

	// State
	Len() int
	IsEmpty() bool
}

type LinkedListNode[T any] interface {
	Value() T
}

type nodeImpl[T any] struct {
	value T
	next  *nodeImpl[T]
}

func (n *nodeImpl[T]) Value() T {
	return n.value
}

func NewLinkedList[T any]() LinkedList[T] {
	return &linkedListImpl[T]{}
}

type linkedListImpl[T any] struct {
	front *nodeImpl[T]
	back  *nodeImpl[T]
	len   int
}

func (l *linkedListImpl[T]) PushFront(val T) {
	tmp := l.front

	n := &nodeImpl[T]{value: val}
	l.front = n
	l.front.next = tmp

	if l.IsEmpty() {
		l.back = l.front
	}

	l.len++
}

func (l *linkedListImpl[T]) PushBack(val T) {
	tmp := l.back
	l.back = &nodeImpl[T]{value: val}
	if tmp != nil {
		tmp.next = l.back
	}

	if l.IsEmpty() {
		l.front = l.back
	}

	l.len++
}

func (l *linkedListImpl[T]) PopFront() (T, bool) {
	var zeroVal T
	if l.IsEmpty() {
		return zeroVal, false
	}
	newFront := l.front.next
	tmp := l.front
	l.front = newFront
	l.len--
	return tmp.Value(), true
}

func (l *linkedListImpl[T]) PopBack() (T, bool) {
	var zeroVal T
	if l.IsEmpty() {
		return zeroVal, false
	}
	result := l.front
	if l.Len() == 1 {
		l.front = nil
		l.back = nil
	} else {
		for result.next.next != nil {
			result = result.next
		}
		l.back = result
		result = result.next
	}
	l.len--

	return result.Value(), true
}

func (l *linkedListImpl[T]) Front() (LinkedListNode[T], bool) {
	if l.front == nil {
		return nil, false
	}
	return l.front, true
}

func (l *linkedListImpl[T]) Back() (LinkedListNode[T], bool) {
	if l.back == nil {
		return nil, false
	}
	return l.back, true
}

func (l *linkedListImpl[T]) Next(node LinkedListNode[T]) (LinkedListNode[T], bool) {
	internal, ok := node.(*nodeImpl[T])
	if !ok || internal.next == nil {
		return nil, false
	}
	return internal.next, true
}

func (l *linkedListImpl[T]) IsEmpty() bool {
	return l.len == 0
}

func (l *linkedListImpl[T]) Len() int {
	return l.len
}
