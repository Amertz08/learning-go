package datastructures

type node struct {
	Val  int
	Next *node
	Prev *node
}

type DoubleLinkedList struct {
	Head *node
}

func NewDLL() *DoubleLinkedList {
	return &DoubleLinkedList{}
}

func (dll *DoubleLinkedList) Append(val int) {
	if dll.Head == nil {
		dll.Head = &node{Val: val}
		return
	}
	current := dll.Head
	for current.Next != nil {
		current = current.Next
	}
	current.Next = &node{Val: val, Prev: current}
}
