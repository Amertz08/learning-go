package linked_list_test

import (
	"testing"

	"github.come/Amertz08/learning-go/datastructures"
)

func TestLinkedListQueueImpl_Len(t *testing.T) {
	t.Run("empty queue", func(t *testing.T) {
		q := datastructures.NewLinkedListQueue[int]()

		if q.Len() != 0 {
			t.Errorf("expected len 0 got %d", q.Len())
		}
	})
}

func TestLinkedListQueueImpl_Enqueue(t *testing.T) {
	t.Run("single item adjusts length", func(t *testing.T) {
		q := datastructures.NewLinkedListQueue[int]()

		q.Enqueue(1)

		if q.Len() != 1 {
			t.Errorf("expected len=1 got %d", q.Len())
		}
	})
}

func TestLinkedListQueueImpl_Dequeue(t *testing.T) {
	t.Run("single item", func(t *testing.T) {
		q := datastructures.NewLinkedListQueue[int]()

		exp := 1
		q.Enqueue(exp)
		obs, ok := q.Dequeue()
		if !ok {
			t.Fatalf("expected ok=true got false")
		}
		if obs != exp {
			t.Fatalf("expected %d got %d", exp, obs)
		}
		if q.Len() != 0 {
			t.Errorf("expected len=0 got %d", q.Len())
		}
	})
}

func TestLinkedListQueueImpl_Peek(t *testing.T) {
	t.Run("empty list", func(t *testing.T) {
		q := datastructures.NewLinkedListQueue[int]()
		var zeroVal int

		val, ok := q.Peek()
		if ok {
			t.Fatalf("expected ok=false got true")
		}
		if val != zeroVal {
			t.Errorf("expected val to be zero value for type")
		}
	})
	t.Run("has item", func(t *testing.T) {
		q := datastructures.NewLinkedListQueue[int]()
		exp := 1

		q.Enqueue(exp)

		val, ok := q.Peek()
		if !ok {
			t.Fatalf("expected ok=true got false")
		}
		if val != exp {
			t.Errorf("expected %d got %d", exp, val)
		}
	})
}

func TestLinkedListQueueImpl_IsEmpty(t *testing.T) {
	t.Run("empty queue", func(t *testing.T) {
		q := datastructures.NewLinkedListQueue[int]()

		if !q.IsEmpty() {
			t.Fatalf("expected IsEmpty=true")
		}
	})
	t.Run("non empty queue", func(t *testing.T) {
		q := datastructures.NewLinkedListQueue[int]()

		q.Enqueue(2)

		if q.IsEmpty() {
			t.Fatalf("expected IsEmpty=false")
		}
	})
}
