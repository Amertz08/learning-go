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
	Head *Node
	size int
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
	return dll.Head
}

func (dll *DoubleLinkedList) PushEnd(val int) {
	if dll.Head == nil {
		dll.Head = &Node{Val: val}
		dll.size++
		return
	}
	current := dll.Head
	for current.Next != nil {
		current = current.Next
	}
	current.Next = &Node{Val: val, Prev: current}
	dll.size++
}

func (dll *DoubleLinkedList) PushFront(val int) {
	if dll.Head == nil {
		dll.Head = &Node{Val: val}
		dll.size++
		return
	}

	tmp := dll.Head
	dll.Head = &Node{Val: val, Next: tmp}
	dll.Head.Next = tmp
	tmp.Prev = dll.Head

	dll.size++
}
