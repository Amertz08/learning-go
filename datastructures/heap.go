package datastructures

import (
	"iter"
)

// TODO: negative values? I am of the mindset you disallow. It's a priority queue
// 		at the end of the day. What is a negative priority? If you want to inject a value
//		to the front of the queue then you simply need a priority < current min.

type Heap[T any] interface {
	Add(value T)
	Pop() T
	List() []T
	Iterate() iter.Seq[T]
}

// left child 2 * i
// right child 2 * i + 1
// parent i / 2

type CompareFunc[T any] func(x T, y T) bool

// TODO: what about a struct type?
//		In a real world application you'd likely be using a struct that contains values.
//		You might want to use one or more of those values to determine priority.
//		So maybe part of the constructor also accepts a comparison function to determine
//		which is lt/gt the other.

func CompareHeap[T any](compFunc CompareFunc[T]) Heap[T] {
	var zeroVal T
	return &compareHeapImpl[T]{data: []T{zeroVal}, compare: compFunc}
}

type compareHeapImpl[T any] struct {
	data    []T
	compare CompareFunc[T]
}

func (h *compareHeapImpl[T]) Add(value T) {
	// Add the value as the newest node
	h.data = append(h.data, value)
	i := len(h.data) - 1

	// percolate up the value
	for h.compare(h.data[i], h.data[i/2]) {
		tmp := h.data[i]
		h.data[i] = h.data[i/2]
		h.data[i/2] = tmp
		i = i / 2
	}
}

func (h *compareHeapImpl[T]) Pop() T {
	var zeroVal T
	if len(h.data) == 1 {
		return zeroVal
	}
	// TODO: actually implement
	return zeroVal
}

func (h *compareHeapImpl[T]) List() []T {
	return h.data[1:]
}

func (h *compareHeapImpl[T]) Iterate() iter.Seq[T] {
	return nil
}
