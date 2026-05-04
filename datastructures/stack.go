package datastructures

/*
Use cases for a stack
- Last in first out (LIFO)
- Reversing order of elements
- Undo/Redo operations
- Expression evaluation (e.g., postfix notation i.e. , reverse polish notation)
- Function call stack in programming languages
*/

type Stack[T any] interface {
	// Push adds a value to the top of the stack
	Push(value T)

	// Pop removes and returns the top value
	// ok is false if the stack is empty
	Pop() (value T, ok bool)

	// Peek returns the top value without removing it
	// ok is false if the stack is empty
	Peek() (value T, ok bool)

	// Len returns the number of elements in the stack
	Len() int

	// IsEmpty reports whether the stack has no elements
	IsEmpty() bool
}

type stackSliceImpl[T any] struct {
	items []T
}

func NewSliceStack[T any]() Stack[T] {
	return &stackSliceImpl[T]{}
}

func (s *stackSliceImpl[T]) Len() int {
	return len(s.items)
}

func (s *stackSliceImpl[T]) Push(value T) {
	s.items = append(s.items, value)
}

func (s *stackSliceImpl[T]) IsEmpty() bool {
	return len(s.items) == 0
}

func (s *stackSliceImpl[T]) Pop() (T, bool) {
	var val T
	if s.IsEmpty() {
		return val, false
	}
	val = s.items[len(s.items)-1]
	s.items = s.items[:len(s.items)-1]
	return val, true
}

func (s *stackSliceImpl[T]) Peek() (T, bool) {
	var val T
	if s.IsEmpty() {
		return val, false
	}
	val = s.items[len(s.items)-1]
	return val, true
}

type linkedListStackImpl[T any] struct {
	items LinkedList[T]
}

func NewLinkedListStack[T any]() Stack[T] {
	return &linkedListStackImpl[T]{items: NewLinkedList[T]()}
}

func (s *linkedListStackImpl[T]) Len() int {
	return s.items.Len()
}
func (s *linkedListStackImpl[T]) IsEmpty() bool { return s.items.IsEmpty() }

func (s *linkedListStackImpl[T]) Push(value T) {
	s.items.PushFront(value)
}

func (s *linkedListStackImpl[T]) Pop() (T, bool) {
	return s.items.PopFront()
}

func (s *linkedListStackImpl[T]) Peek() (T, bool) {
	node, ok := s.items.Front()
	if !ok {
		var val T
		return val, false
	}
	return node.Value(), true
}
