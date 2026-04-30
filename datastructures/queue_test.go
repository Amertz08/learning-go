package datastructures

import "testing"

func TestQueue_Len(t *testing.T) {
	t.Run("empty list", func(t *testing.T) {
		q := queueImp[int]{}

		if q.Len() != 0 {
			t.Errorf("expected 0 on empty queue got: %d", q.Len())
		}
	})
	t.Run("queue with one item", func(t *testing.T) {
		q := queueImp[int]{}

		q.Enqueue(1)

		if q.Len() != 1 {
			t.Errorf("expected 1 on queue got: %d", q.Len())
		}
	})
}
