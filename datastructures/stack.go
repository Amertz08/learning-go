package datastructures

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

type stackImp[T any] struct {
	items []T
}

func (s *stackImp[T]) Len() int {
	return len(s.items)
}

func (s *stackImp[T]) Push(value T) {
	s.items = append(s.items, value)
}

func (s *stackImp[T]) IsEmpty() bool {
	return len(s.items) == 0
}

func (s stackImp[T]) Pop() (T, bool) {
	var val T
	if s.IsEmpty() {
		return val, false
	}
	val = s.items[len(s.items)-1]
	s.items = s.items[:len(s.items)-1]
	return val, true
}

func (s stackImp[T]) Peak() (T, bool) {
	var val T
	if s.IsEmpty() {
		return val, false
	}
	val = s.items[len(s.items)-1]
	return val, true
}
