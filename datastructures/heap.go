package datastructures

import "golang.org/x/exp/constraints"

type Heap[T constraints.Ordered] interface {
	Add(value T)
	Pop() T
	List() []T
}

// left child 2 * i
// right child 2 * i + 1
// parent i / 2
