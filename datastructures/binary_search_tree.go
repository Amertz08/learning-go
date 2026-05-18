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

type bstNodeImpl[T constraints.Ordered] struct {
	val T
}

func NewBSTNodeImpl[T constraints.Ordered](val T) BSTNode[T] {
	return &bstNodeImpl[T]{val}
}

func (n *bstNodeImpl[T]) Value() T {
	return n.val
}

func (n *bstNodeImpl[T]) Left() (BSTNode[T], bool) {
	return nil, false
}

func (n *bstNodeImpl[T]) Right() (BSTNode[T], bool) {
	return nil, false
}

func (n *bstNodeImpl[T]) HasLeft() bool {
	return false
}

func (n *bstNodeImpl[T]) HasRight() bool {
	return false
}
