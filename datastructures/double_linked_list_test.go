package datastructures

import "testing"

func TestAppend(t *testing.T) {
	t.Run("append_single_element", func(t *testing.T) {
		l := NewDLL()
		l.Append(1)

		if l.Head == nil {
			t.Errorf("Expected head to be set after append, got nil")
		}
		if l.Head.Val != 1 {
			t.Errorf("Expected head value to be 1, got %d", l.Head.Val)
		}
	})
	t.Run("append_multiple_elements", func(t *testing.T) {
		l := NewDLL()
		l.Append(1)
		l.Append(2)

		if l.Head == nil {
			t.Errorf("Expected head to be set after append, got nil")
		}
		if l.Head.Val != 1 {
			t.Errorf("Expected head value to be 1, got %d", l.Head.Val)
		}
		if l.Head.Next == nil {
			t.Errorf("Expected second node to be set after append, got nil")
		}
		if l.Head.Next.Val != 2 {
			t.Errorf("Expected second node value to be 2, got %d", l.Head.Next.Val)
		}
	})
}

func TestSize(t *testing.T) {
	t.Run("empty_list", func(t *testing.T) {
		l := NewDLL()
		if l.Size() != 0 {
			t.Errorf("Expected size of empty list to be 0, got %d", l.Size())
		}
	})
	t.Run("only_one_element", func(t *testing.T) {
		l := NewDLL()
		l.Append(1)
		if l.Size() != 1 {
			t.Errorf("Expected size of list with 1 element to be 1, got %d", l.Size())
		}
	})
	t.Run("non_empty_list", func(t *testing.T) {
		l := NewDLL()
		l.Append(1)
		l.Append(2)
		if l.Size() != 2 {
			t.Errorf("Expected size of list with 2 elements to be 2, got %d", l.Size())
		}
	})
}
