package datastructures

import "fmt"

// Asked ChatGPT for an interface, so I knew what methods to implement.
type DoublyLinkedList[T any] interface {
	// Insert
	PushFront(value T)
	PushBack(value T)
	InsertAfter(node DoubleLinkedListNode[T], value T) DoubleLinkedListNode[T]
	InsertBefore(node DoubleLinkedListNode[T], value T) DoubleLinkedListNode[T]

	// Remove
	PopFront() (value T, ok bool)
	PopBack() (value T, ok bool)
	Remove(node DoubleLinkedListNode[T]) (value T, ok bool)

	// Access
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
