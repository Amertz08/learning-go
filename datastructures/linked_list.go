package datastructures

type LinkedList[T any] interface {
	// Insert
	PushFront(value T)
	PushBack(value T)
	InsertAfter(node LinkedListNode[T], value T) LinkedListNode[T]

	// Remove
	PopFront() (value T, ok bool)
	RemoveAfter(node LinkedListNode[T]) (value T, ok bool)

	// Access
	Front() LinkedListNode[T]
	Back() LinkedListNode[T]

	// Navigation
	Next(node LinkedListNode[T]) LinkedListNode[T]

	// State
	Len() int
	IsEmpty() bool
}

type LinkedListNode[T any] interface {
	Value() T
}

type nodeImpl[T any] struct {
	val  T
	next LinkedListNode[T]
}

func (n *nodeImpl[T]) Value() T {
	return n.val
}

type linkedListImpl[T any] struct {
	front LinkedListNode[T]
}

func (l *linkedListImpl[T]) IsEmpty() bool {
	return l.front == nil
}

func (l *linkedListImpl[T]) Front() LinkedListNode[T] {
	return l.front
}

func (l *linkedListImpl[T]) PushFront(val T) {
	n := &nodeImpl[T]{val: val}
	l.front = n
}
