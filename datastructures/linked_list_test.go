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

func TestLinkedListImpl_Front(t *testing.T) {
	t.Run("empty list return bool", func(t *testing.T) {
		l := linkedListImpl[int]{}
		var zeroVal int

		val, ok := l.Front()

		if ok {
			t.Errorf("expected false got true")
		}

		if val != zeroVal {
			t.Errorf("expected zero val got: %d", val)
		}
	})
	t.Run("non empty list returns actual value", func(t *testing.T) {
		l := linkedListImpl[int]{}

		exp := 1
		l.PushFront(exp)
		val, ok := l.Front()
		if !ok {
			t.Errorf("expected true got false")
		}
		if val != exp {
			t.Errorf("got %d, exp %d", val, exp)
		}
	})
}

func TestLInkedListImp_PushFront(t *testing.T) {
	t.Run("push a single value", func(t *testing.T) {
		l := linkedListImpl[int]{}

		exp := 1
		l.PushFront(exp)

		front, ok := l.Front()
		if !ok {
			t.Fatalf("expected a value from front")
		}
		if front != exp {
			t.Errorf("expected %d, got %d", exp, front)
		}
	})
	t.Run("push multiple values", func(t *testing.T) {
		l := linkedListImpl[int]{}

		exp := 2
		l.PushFront(1)
		l.PushFront(exp)

		front, ok := l.Front()
		if !ok {
			t.Fatalf("expected a value from front")
		}
		if l.Len() != 2 {
			t.Fatalf("expected a length of 2 got: %d", l.Len())
		}
		if front != exp {
			t.Fatalf("expected %d, got %d", exp, front)
		}
	})
}
