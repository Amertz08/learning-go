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
	val   T
	left  *bstNodeImpl[T]
	right *bstNodeImpl[T]
}

func NewBSTNodeImpl[T constraints.Ordered](val T) BSTNode[T] {
	return &bstNodeImpl[T]{val, nil, nil}
}

func (n *bstNodeImpl[T]) Value() T {
	return n.val
}

func (n *bstNodeImpl[T]) Left() (BSTNode[T], bool) {
	if n.left != nil {
		return n.left, true
	}
	return n.left, false
}

func (n *bstNodeImpl[T]) Right() (BSTNode[T], bool) {
	if n.right != nil {
		return n.right, true
	}
	return n.right, false
}

func (n *bstNodeImpl[T]) HasLeft() bool {
	return n.left != nil
}

func (n *bstNodeImpl[T]) HasRight() bool {
	return n.right != nil
}
