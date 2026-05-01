package datastructures

import "testing"

func TestLinkedListImp_IsEmpty(t *testing.T) {
	t.Run("empty list", func(t *testing.T) {
		l := linkedListImpl[int]{}

		if !l.IsEmpty() {
			t.Errorf("expected an empty list got true")
		}
	})
	t.Run("non empty list", func(t *testing.T) {
		l := linkedListImpl[int]{}

		l.PushFront(1)

		if l.IsEmpty() {
			t.Errorf("expected a non empty list got false")
		}
	})
}

func TestLInkedListImp_PushFront(t *testing.T) {
	t.Run("push a single value", func(t *testing.T) {
		l := linkedListImpl[int]{}

		exp := 1
		l.PushFront(exp)

		front := l.Front()
		if front == nil {
			t.Fatalf("expected a node got nil")
		}
		if front.Value() != exp {
			t.Errorf("expected %d, got %d", exp, front.Value())
		}
	})
}
