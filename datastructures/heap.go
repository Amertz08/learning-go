package datastructures

import (
	"iter"

	"golang.org/x/exp/constraints"
)

type Heap[T constraints.Ordered] interface {
	Add(value T)
	Pop() T
	List() []T
	Iterate() iter.Seq[T]
}

// left child 2 * i
// right child 2 * i + 1
// parent i / 2

func NewMinHeap[T constraints.Ordered]() Heap[T] {
	var zeroVal T
	return &minHeapImpl[T]{data: []T{zeroVal}}
}

type minHeapImpl[T constraints.Ordered] struct {
	data []T
}

func (h *minHeapImpl[T]) Add(value T) {
	h.data = append(h.data, value)
}

func (h *minHeapImpl[T]) Pop() T {
	var zeroVal T
	return zeroVal
}

func (h *minHeapImpl[T]) List() []T {
	return h.data[1:]
}

func (h *minHeapImpl[T]) Iterate() iter.Seq[T] {
	return nil
}
