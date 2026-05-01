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
	Next() LinkedListNode[T]
	SetNext(node LinkedListNode[T])
}

type nodeImpl[T any] struct {
	val  T
	next LinkedListNode[T]
}

func (n *nodeImpl[T]) Value() T {
	return n.val
}

func (n *nodeImpl[T]) Next() LinkedListNode[T] {
	return n.next
}

func (n *nodeImpl[T]) SetNext(node LinkedListNode[T]) {
	n.next = node
}

type linkedListImpl[T any] struct {
	front LinkedListNode[T]
	len   int
}

func (l *linkedListImpl[T]) IsEmpty() bool {
	return l.front == nil
}

func (l *linkedListImpl[T]) Len() int {
	return l.len
}

func (l *linkedListImpl[T]) Front() LinkedListNode[T] {
	return l.front
}

func (l *linkedListImpl[T]) PushFront(val T) {
	tmp := l.front

	n := &nodeImpl[T]{val: val}
	l.front = n
	l.front.SetNext(tmp)
	l.len++
}
