package datastructures

type LinkedList[T any] interface {
	// Insert
	PushFront(value T)
	PushBack(value T)
	InsertAt(index int, value T)

	// Remove
	PopFront() (value T, ok bool)
	RemoveAfter(index int) (value T, ok bool)

	// Access
	Front() (T, bool)
	Back() T

	// State
	Len() int
	IsEmpty() bool
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
	len   int
}

func (l *linkedListImpl[T]) PushFront(val T) {
	tmp := l.front

	n := &nodeImpl[T]{Value: val}
	l.front = n
	l.front.Next = tmp
	l.len++
}

func (l *linkedListImpl[T]) Front() (T, bool) {
	var zeroValue T
	if l.front == nil {
		return zeroValue, false
	}
	return l.front.Value, true
}

func (l *linkedListImpl[T]) IsEmpty() bool {
	return l.front == nil
}

func (l *linkedListImpl[T]) Len() int {
	return l.len
}
