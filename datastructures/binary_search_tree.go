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

type bstImpl[T constraints.Ordered] struct {
	root *bstNodeImpl[T]
	len  int
}

func NewBSTImpl[T constraints.Ordered]() BinarySearchTree[T] {
	return &bstImpl[T]{}
}

func (t *bstImpl[T]) Insert(value T) bool {
	if t.root == nil {
		t.root = &bstNodeImpl[T]{val: value}
		t.len++
		return true
	}

	var ok bool
	if value < t.root.Value() {
		// insert left
		t.root.left, ok = t.insert(t.root.left, value)
	} else if value > t.root.Value() {
		// insert right
		t.root.right, ok = t.insert(t.root.right, value)
	} else {
		// same value
	}
	return ok
}

func (t *bstImpl[T]) insert(node *bstNodeImpl[T], value T) (*bstNodeImpl[T], bool) {
	if node == nil {
		t.len++
		return &bstNodeImpl[T]{val: value}, true
	}
	if value < node.Value() {
		return t.insert(node.left, value)
	} else if value > node.Value() {
		return t.insert(node.right, value)
	}

	return nil, false
}

func (t *bstImpl[T]) Remove(value T) bool {
	return false
}

func (t *bstImpl[T]) Clear() {}

func (t *bstImpl[T]) Contains(value T) bool {
	if t.root != nil {
		return t.root.val == value
	}
	return false
}

func (t *bstImpl[T]) Find(value T) (BSTNode[T], bool) {
	if t.root != nil {
		if t.root.Value() == value {
			return t.root, true
		}
	}
	return nil, false
}

func (t *bstImpl[T]) Root() (BSTNode[T], bool) {
	if t.root != nil {
		return t.root, true
	}
	return nil, false
}

func (t *bstImpl[T]) Min() (T, bool) {
	if t.root != nil {
		return t.root.Value(), true
	}
	var zeroVal T
	return zeroVal, false
}

func (t *bstImpl[T]) Max() (T, bool) {
	if t.root != nil {
		return t.root.Value(), true
	}
	var zeroVal T
	return zeroVal, false
}

func (t *bstImpl[T]) InOrder() []T {
	var vals []T
	return t.inOrder(t.root, vals)
}

func (t *bstImpl[T]) inOrder(node *bstNodeImpl[T], vals []T) []T {
	if node == nil {
		return vals
	}
	vals = t.inOrder(node.left, vals)
	vals = append(vals, node.Value())
	vals = t.inOrder(node.right, vals)
	return vals
}

func (t *bstImpl[T]) PreOrder() []T {
	if t.root != nil {
		return []T{t.root.Value()}
	}
	return nil
}

func (t *bstImpl[T]) PostOrder() []T {
	if t.root != nil {
		return []T{t.root.Value()}
	}
	return nil
}

func (t *bstImpl[T]) LevelOrder() []T {
	if t.root != nil {
		return []T{t.root.Value()}
	}
	return nil
}

func (t *bstImpl[T]) Len() int {
	return t.len
}

func (t *bstImpl[T]) IsEmpty() bool {
	return t.root == nil
}
