package datastructures

// Asked ChatGPT for an interface, so I knew what methods to implement.
type DoublyLinkedList[T any] interface {
	// Insert
	PushFront(value T)
	PushBack(value T)
	//InsertAfter(node DoubleLinkedListNode[T], value T) DoubleLinkedListNode[T]
	//InsertBefore(node DoubleLinkedListNode[T], value T) DoubleLinkedListNode[T]

	// Remove
	PopFront() (value T, ok bool)
	PopBack() (value T, ok bool)
	Remove(node DoubleLinkedListNode[T]) (value T, ok bool)

	// Access
	// TODO: I am not understanding why we need to return an 'ok' here.
	//	The existence of an 'ok' on here and the node methods seems to imply
	//	that the underlying variables we interact with on the concrete implemenation
	//	should have the interface as it's type. If that were the case then we would
	// 	need to do a type conversion in order to act on the concrete implemenation
	//	variables. However to my understanding the concrete list should use the concrete
	//	node implemenation which should also use the concrete node implementation for it's
	// 	pointers. Given they would be concret there is no need to convert the type and thus no
	//	need for the 'ok' check. Given these were generate by ChatGPT they could be wrong.
	//	At least according to ChatGPT this is perfectly fine and how I implemented the methods
	//	is the same as what it generated when asked.
	Front() (DoubleLinkedListNode[T], bool)
	Back() (DoubleLinkedListNode[T], bool)

	// State
	Len() int
	IsEmpty() bool
}

type DoubleLinkedListNode[T any] interface {
	Value() T
	Next() (DoubleLinkedListNode[T], bool)
	Prev() (DoubleLinkedListNode[T], bool)
}

type doubleLinkedListNodeImpl[T any] struct {
	val  T
	next *doubleLinkedListNodeImpl[T]
	prev *doubleLinkedListNodeImpl[T]
}

func (n *doubleLinkedListNodeImpl[T]) Value() T {
	return n.val
}

func (n *doubleLinkedListNodeImpl[T]) Next() (DoubleLinkedListNode[T], bool) {
	if n.next != nil {
		return n.next, true
	}
	return n.next, false
}

func (n *doubleLinkedListNodeImpl[T]) Prev() (DoubleLinkedListNode[T], bool) {
	if n.prev != nil {
		return n.prev, true
	}
	return n.prev, false
}

type doubleLinkedListImpl[T any] struct {
	len   int
	front *doubleLinkedListNodeImpl[T]
	back  *doubleLinkedListNodeImpl[T]
}

func NewGenericDoubleLinkedList[T any]() DoublyLinkedList[T] {
	return &doubleLinkedListImpl[T]{}
}

func (l *doubleLinkedListImpl[T]) Len() int {
	return l.len
}

func (l *doubleLinkedListImpl[T]) IsEmpty() bool {
	return l.len == 0
}

func (l *doubleLinkedListImpl[T]) PushFront(value T) {
	newFront := &doubleLinkedListNodeImpl[T]{val: value}

	// get the old front and swap it around if we can
	oldFront := l.front
	if oldFront != nil {
		oldFront.prev = newFront
	}

	// reassign the front to the new node
	newFront.next = oldFront
	l.front = newFront

	// If the list was empty, front == back
	if l.IsEmpty() {
		l.back = newFront
	}

	l.len++
}

func (l *doubleLinkedListImpl[T]) PushBack(value T) {
	newBack := &doubleLinkedListNodeImpl[T]{val: value}

	// get whatever is the current back and set it's next to the new one
	oldBack := l.back
	if oldBack != nil {
		oldBack.next = newBack
	}

	// reassign the back and point to whatever was there prior
	l.back = newBack
	l.back.prev = oldBack

	// If the list was empty, front == back
	if l.IsEmpty() {
		l.front = newBack
	}

	l.len++
}

func (l *doubleLinkedListImpl[T]) PopFront() (value T, ok bool) {
	var zeroVal T
	if l.IsEmpty() {
		return zeroVal, false
	}

	// Get what we're going to return and reassign the new front
	oldFront := l.front
	newFront := oldFront.next
	l.front = newFront

	// If there is a node we need to have it point to nil
	if l.front != nil {
		l.front.prev = nil
	}

	// We do not need to do oldFront.next = nil as there are no more references to the node itself
	// and the node will be cleaned up by the garbage collector

	l.len--
	return oldFront.Value(), true
}

func (l *doubleLinkedListImpl[T]) PopBack() (value T, ok bool) {
	var zeroVal T
	if l.IsEmpty() {
		return zeroVal, false
	}
	oldBack := l.back
	newBack := oldBack.prev
	l.back = newBack

	// If the new back exists we need to point it's next to nil
	if l.back != nil {
		l.back.next = nil
	}

	l.len--
	return oldBack.Value(), true
}

func (l *doubleLinkedListImpl[T]) Remove(node DoubleLinkedListNode[T]) (value T, ok bool) {
	var zeroVal T

	// Not really sure how you could get here on an empty list given
	// there is no public method for creating a node. Have this JIC.
	if l.IsEmpty() {
		return zeroVal, false
	}
	if l.Len() == 1 {
		return l.PopFront()
	}

	if node == l.front {
		return l.PopFront()
	}
	if node == l.back {
		return l.PopBack()
	}

	// TODO: we might want to handle !ok here
	prev, _ := node.Prev()
	p, _ := prev.(*doubleLinkedListNodeImpl[T])
	next, _ := node.Next()
	n, _ := next.(*doubleLinkedListNodeImpl[T])
	if p != nil {
		p.next = n
	}
	if n != nil {
		n.prev = p
	}
	l.len--
	return node.Value(), true
}

func (l *doubleLinkedListImpl[T]) Front() (DoubleLinkedListNode[T], bool) {
	if l.front != nil {
		return l.front, true
	}
	return l.front, false
}

func (l *doubleLinkedListImpl[T]) Back() (DoubleLinkedListNode[T], bool) {
	if l.back != nil {
		return l.back, true
	}
	return l.back, false
}
