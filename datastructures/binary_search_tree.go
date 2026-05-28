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
	var ok bool
	t.root, ok = t.insert(t.root, value)
	return ok
}

func (t *bstImpl[T]) insert(node *bstNodeImpl[T], value T) (*bstNodeImpl[T], bool) {
	if node == nil {
		t.len++
		return &bstNodeImpl[T]{val: value}, true
	}

	var ok bool
	if value < node.Value() {
		node.left, ok = t.insert(node.left, value)
		return node, ok
	} else if value > node.Value() {
		node.right, ok = t.insert(node.right, value)
		return node, ok
	}

	return nil, false
}

func (t *bstImpl[T]) Remove(value T) bool {
	var found bool
	t.root, found = t.remove(t.root, value)
	return found
}

func (t *bstImpl[T]) remove(node *bstNodeImpl[T], value T) (*bstNodeImpl[T], bool) {
	var found bool
	if node == nil {
		return nil, found
	}

	if value < node.Value() {
		node.left, found = t.remove(node.left, value)
		return node, found
	} else if value > node.Value() {
		node.right, found = t.remove(node.right, value)
		return node, found
	} else {
		found = true

		// If neither tree exists, we can just return now and the caller
		// can reassign to nil
		if node.left == nil && node.right == nil {
			return nil, found
		}
		// If the left exists but not the right just return left
		if node.left != nil && node.right == nil {
			return node.left, found
		}
		// If the right exists but not the left just return right
		if node.left == nil && node.right != nil {
			return node.right, found
		}
		leftMaxPtr := node.left.right

		// If the right value of the left subtree is already nill we know
		// we can just set the right subtree to be the right of the left subtree
		if leftMaxPtr == nil {
			node.left.right = node.right
			return node.left, found
		}

		// traverse to the max value in the left subtree
		for leftMaxPtr.right != nil {
			leftMaxPtr = leftMaxPtr.right
		}
		// assign it's right pointer to be the right tree
		leftMaxPtr.right = node.right
		return node.left, found
	}
}

func (t *bstImpl[T]) Clear() {}

func (t *bstImpl[T]) Contains(value T) bool {
	return t.contains(t.root, value)
}

func (t *bstImpl[T]) contains(node *bstNodeImpl[T], value T) bool {
	if node == nil {
		return false
	}
	if value < node.Value() {
		return t.contains(node.left, value)
	} else if value > node.Value() {
		return t.contains(node.right, value)
	}
	// same value
	return true
}

func (t *bstImpl[T]) Find(value T) (BSTNode[T], bool) {
	return t.find(t.root, value)
}

func (t *bstImpl[T]) find(node *bstNodeImpl[T], value T) (BSTNode[T], bool) {
	if node == nil {
		return nil, false
	}
	if value < node.Value() {
		return t.find(node.left, value)
	} else if value > node.Value() {
		return t.find(node.right, value)
	}
	return node, true
}

func (t *bstImpl[T]) Root() (BSTNode[T], bool) {
	if t.root != nil {
		return t.root, true
	}
	return nil, false
}

func (t *bstImpl[T]) Min() (T, bool) {
	var minVal T
	if t.root == nil {
		return minVal, false
	}
	curPtr := t.root
	minVal = curPtr.Value()
	for curPtr, ok := curPtr.Left(); ok; {
		minVal = curPtr.Value()
		curPtr, ok = curPtr.Left()
	}

	return minVal, true
}

func (t *bstImpl[T]) Max() (T, bool) {
	var maxVal T
	if t.root == nil {
		return maxVal, false
	}
	curPtr := t.root
	maxVal = curPtr.Value()
	for curPtr, ok := curPtr.Right(); ok; {
		maxVal = curPtr.Value()
		curPtr, ok = curPtr.Right()
	}
	return maxVal, true
}

func (t *bstImpl[T]) InOrder() []T {
	vals := make([]T, 0)
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
	vals := make([]T, 0)
	return t.preOrder(t.root, vals)
}

func (t *bstImpl[T]) preOrder(node *bstNodeImpl[T], vals []T) []T {
	if node == nil {
		return vals
	}
	vals = append(vals, node.Value())
	vals = t.preOrder(node.left, vals)
	vals = t.preOrder(node.right, vals)

	return vals
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
