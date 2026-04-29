package datastructures

/*
Asked ChatGPT for an interface, so I knew what methods to impelment
type DoublyLinkedList[T any] interface {
    // Insert
    PushFront(value T)
    PushBack(value T)
    InsertBefore(node Node[T], value T) Node[T]
    InsertAfter(node Node[T], value T) Node[T]

    // Remove
    Remove(node Node[T]) T

    // Access
    Front() Node[T]
    Back() Node[T]

    // Navigation
    Next(node Node[T]) Node[T]
    Prev(node Node[T]) Node[T]

    // State
    Len() int // Done
    IsEmpty() bool
}

type Node[T any] interface {
    Value() T
}
*/

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

func NewDLL() *DoubleLinkedList {
	return &DoubleLinkedList{}
}

func (dll *DoubleLinkedList) Size() int {
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

func (dll *DoubleLinkedList) PushEnd(val int) {
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
		dll.size++
		return
	}

	tmp := dll.front
	dll.front = &Node{Val: val, Next: tmp}
	dll.front.Next = tmp
	tmp.Prev = dll.front

	dll.size++
}
