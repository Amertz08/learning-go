package datastructures

import (
	"iter"

	"golang.org/x/exp/constraints"
)

// TODO: negative values? I am of the mindset you disallow. It's a priority queue
// 		at the end of the day. What is a negative priority? If you want to inject a value
//		to the front of the queue then you simply need a priority < current min.

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
	// Add the value as the newest node
	h.data = append(h.data, value)
	i := len(h.data) - 1

	// percolate up the value
	for h.data[i] < h.data[i/2] {
		tmp := h.data[i]
		h.data[i] = h.data[i/2]
		h.data[i/2] = tmp
		i = i / 2
	}
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
