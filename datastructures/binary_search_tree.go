package datastructures

import "golang.org/x/exp/constraints"

type BinarySearchTree[T constraints.Ordered] interface {
	// Insert
	Insert(value T) bool

	// Remove
	Remove(value T) bool
	Clear()

	// Search
	Contains(value T) bool
	Find(value T) (BSTNode[T], bool)

	// Access
	Root() (BSTNode[T], bool)
	Min() (T, bool)
	Max() (T, bool)

	// Traversal
	InOrder() []T
	PreOrder() []T
	PostOrder() []T
	LevelOrder() []T

	// State
	Len() int
	IsEmpty() bool
}

type BSTNode[T constraints.Ordered] interface {
	Value() T

	Left() (BSTNode[T], bool)
	Right() (BSTNode[T], bool)

	HasLeft() bool
	HasRight() bool
}
