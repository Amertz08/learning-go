package datastructures

import "fmt"

/*
Asked ChatGPT for an interface, so I knew what methods to implement.
TODO: remove returning of node and instead return value
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
	Get(idx int) Node[T]

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
