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

func TestLinkedListImp_PushFront(t *testing.T) {
	t.Run("push a single value", func(t *testing.T) {
		l := linkedListImpl[int]{}

		exp := 1
		l.PushFront(exp)

		if l.Len() != 1 {
			t.Fatalf("expected a len of 1 got: %d", l.Len())
		}

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
	t.Run("front is pointing at next in line", func(t *testing.T) {
		// TODO: we need the methods to do this
	})
}

func TestLinkedListImpl_PushBack(t *testing.T) {
	t.Run("push a single value on an empty list", func(t *testing.T) {
		l := linkedListImpl[int]{}

		exp := 1
		l.PushBack(exp)

		if l.Len() != 1 {
			t.Fatalf("expected a len of 1 got: %d", l.Len())
		}

		obs, ok := l.Back()

		if !ok {
			t.Fatalf("expected ok=false got true")
		}
		if obs != exp {
			t.Errorf("got %d, want %d", obs, exp)
		}
	})
	t.Run("push multiple values", func(t *testing.T) {
		l := linkedListImpl[int]{}

		exp := 1
		l.PushBack(2)
		l.PushBack(exp)

		if l.Len() != 2 {
			t.Fatalf("expected len=2 got %d", l.Len())
		}

		obs, ok := l.Back()
		if !ok {
			t.Fatalf("expected ok=true got false")
		}
		if obs != exp {
			t.Errorf("expected %d got %d", exp, obs)
		}
	})
	t.Run("old back.next points to new back", func(t *testing.T) {
		// TODO: we do not have the methods to test this yet
	})
}

func TestLinkedListImpl_Back(t *testing.T) {
	t.Run("empty list", func(t *testing.T) {
		l := linkedListImpl[int]{}
		var zeroValue int

		val, ok := l.Back()

		if ok {
			t.Fatalf("expected ok=false got true")
		}
		if val != zeroValue {
			t.Errorf("expected %d got %d", zeroValue, val)
		}
	})
	t.Run("one value", func(t *testing.T) {
		l := linkedListImpl[int]{}

		exp := 1
		l.PushBack(exp)

		val, ok := l.Back()

		if !ok {
			t.Fatalf("expected ok=true got false")
		}
		if val != exp {
			t.Errorf("expected %d got %d", exp, val)
		}
	})
	t.Run("push multiple values", func(t *testing.T) {
		l := linkedListImpl[int]{}

		exp := 1
		l.PushBack(2)
		l.PushBack(exp)

		val, ok := l.Back()

		if !ok {
			t.Fatalf("expected ok=true got false")
		}
		if val != exp {
			t.Fatalf("expected %d got %d", exp, val)
		}
	})
}
