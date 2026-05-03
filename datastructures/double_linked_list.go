package datastructures

import "fmt"

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
	l.back = nil

	// TODO: not complete
	//		case: multiple values and we need to reassign the back

	l.len--
	return oldBack.Value(), true
}

func (l *doubleLinkedListImpl[T]) Remove(node DoubleLinkedListNode[T]) (value T, ok bool) {
	//TODO implement me
	panic("implement me")
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

// TODO: make a generic doubly linked list
type Node struct {
	Val  int
	Next *Node
	Prev *Node
}

type DoubleLinkedList struct {
	front *Node
	back  *Node
	size  int
}

func NewDoubleLinkedList() *DoubleLinkedList {
	return &DoubleLinkedList{}
}

func (dll *DoubleLinkedList) Len() int {
	return dll.size
}

func (dll *DoubleLinkedList) IsEmpty() bool {
	// Could also do dll.Head == nil
	return dll.size == 0
}

func (dll *DoubleLinkedList) Front() *Node {
	return dll.front
}

func (dll *DoubleLinkedList) Back() *Node {
	return dll.back
}

func (dll *DoubleLinkedList) PushBack(val int) {
	if dll.front == nil {
		dll.front = &Node{Val: val}
		dll.back = dll.front
		dll.size++
		return
	}

	tmp := dll.back
	dll.back = &Node{Val: val}
	dll.back.Prev = tmp
	tmp.Next = dll.back
	dll.size++
}

func (dll *DoubleLinkedList) PushFront(val int) {
	if dll.front == nil {
		dll.front = &Node{Val: val}
		dll.back = dll.front
		dll.size++
		return
	}

	tmp := dll.front
	dll.front = &Node{Val: val, Next: tmp}
	dll.front.Next = tmp
	tmp.Prev = dll.front

	dll.size++
}

func (dll *DoubleLinkedList) Remove(val int) error {
	// Iterate through the list until the first value is found
	// target previous node to target nodes next
	curPtr := dll.front
	for curPtr != nil {
		if curPtr.Val == val {
			nextPtr := curPtr.Next
			prevPtr := curPtr.Prev

			// Swap node next/prev pointers if possible
			if prevPtr != nil {
				prevPtr.Next = nextPtr
			}
			if nextPtr != nil {
				nextPtr.Prev = prevPtr
			}

			// Handle case we're at the front or back of the list
			if curPtr == dll.front {
				dll.front = nextPtr
			}
			if curPtr == dll.back {
				dll.back = prevPtr
			}

			dll.size--

			return nil
		}
		curPtr = curPtr.Next
	}
	return fmt.Errorf("value not found in list")
}
