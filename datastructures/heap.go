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

// CompareFunc should evaluate less than or greater than depending on whether
// you want a min or a max heap
type CompareFunc[T any] func(x T, y T) bool

func NewCompareHeap[T any](compFunc CompareFunc[T]) Heap[T] {
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

	var val T
	if len(h.data) == 2 {
		val = h.data[1]
		h.data = h.data[:len(h.data)-1]
		return val
	}
	// TODO: tests

	val = h.data[1]

	// Get the last value and assign it as the new root
	newFirst := h.data[len(h.data)-1]
	h.data[1] = newFirst

	// 'pop' the heap i.e. drop the last value since it's now at the root
	h.data = h.data[:len(h.data)-1]

	// Percolate down value
	i := 1
	leftChildIdx := 2 * i
	for leftChildIdx < len(h.data) {
		rightChildIdx := 2*i + 1

		if (rightChildIdx < len(h.data)) && (h.compare(h.data[rightChildIdx], h.data[leftChildIdx])) &&
			(!h.compare(h.data[i], h.data[rightChildIdx])) {
			// swap right child
			tmp := h.data[i]
			h.data[i] = h.data[rightChildIdx]
			h.data[rightChildIdx] = tmp
			i = rightChildIdx
		} else if !h.compare(h.data[i], h.data[leftChildIdx]) {
			// swap left child
			tmp := h.data[i]
			h.data[i] = h.data[leftChildIdx]
			h.data[leftChildIdx] = tmp
			i = leftChildIdx
		} else {
			break
		}
	}
	return val
}

func (h *compareHeapImpl[T]) List() []T {
	return h.data[1:]
}

func (h *compareHeapImpl[T]) Iterate() iter.Seq[T] {
	return func(yield func(T) bool) {
		for i := 1; i < len(h.data); i++ {
			yield(h.data[i])
		}
	}
}
