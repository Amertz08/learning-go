package datastructures

import "testing"

func TestStackImp_Len(t *testing.T) {
	t.Run("should return 0 for empty stack", func(t *testing.T) {
		stack := NewStack[int]()
		if stack.Len() != 0 {
			t.Errorf("Expected length 0, got %d", stack.Len())
		}
	})
	t.Run("should return correct length after push", func(t *testing.T) {
		stack := NewStack[int]()
		stack.Push(1)
		if stack.Len() != 1 {
			t.Errorf("Expected length 1, got %d", stack.Len())
		}
	})
	t.Run("should return correct length after multiple pushes", func(t *testing.T) {
		stack := NewStack[int]()
		stack.Push(1)
		stack.Push(2)
		stack.Push(3)
		if stack.Len() != 3 {
			t.Errorf("Expected length 3, got %d", stack.Len())
		}
	})
}

func TestStackImp_IsEmpty(t *testing.T) {
	t.Run("should return true for empty stack", func(t *testing.T) {
		stack := NewStack[int]()
		if !stack.IsEmpty() {
			t.Errorf("Expected stack to be empty, got non-empty")
		}
	})
	t.Run("should return false for non-empty stack", func(t *testing.T) {
		stack := NewStack[int]()
		stack.Push(1)
		if stack.IsEmpty() {
			t.Errorf("Expected stack to be non-empty, got empty")
		}
	})
}

func TestStackImp_Pop(t *testing.T) {
	t.Run("should return false for empty stack", func(t *testing.T) {
		stack := NewStack[int]()
		_, ok := stack.Pop()
		if ok {
			t.Errorf("Expected false got true for empty stack")
		}
	})
	t.Run("should return true for non empty stack", func(t *testing.T) {
		stack := NewStack[int]()
		stack.Push(1)
		val, ok := stack.Pop()
		if !ok {
			t.Errorf("Expected true got false for non-empty stack")
		}
		if val != 1 {
			t.Errorf("Expected value 1 got %d", val)
		}
	})
}

func TestStackImp_Peak(t *testing.T) {
	t.Run("returns false on empty stack", func(t *testing.T) {
		stack := NewStack[int]()
		_, ok := stack.Peek()
		if ok {
			t.Errorf("Expected false got true for empty stack")
		}
	})
	t.Run("returns value at end of stack", func(t *testing.T) {
		stack := NewStack[int]()
		stack.Push(1)
		val, ok := stack.Peek()
		if !ok {
			t.Errorf("Expected true got false for non-empty stack")
		}
		if val != 1 {
			t.Errorf("Expected value 1 got %d", val)
		}
	})
}
