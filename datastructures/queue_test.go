package datastructures

import "testing"

func TestQueue_Len(t *testing.T) {
	t.Run("empty queue", func(t *testing.T) {
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

func TestQueue_IsEmpty(t *testing.T) {
	t.Run("empty queue", func(t *testing.T) {
		q := queueImp[int]{}

		ok := q.IsEmpty()
		if !ok {
			t.Errorf("expected true got false")
		}
	})
	t.Run("non empty queue", func(t *testing.T) {
		q := queueImp[int]{}

		q.Enqueue(1)
		ok := q.IsEmpty()
		if ok {
			t.Errorf("expected false got true")
		}
	})
}

func TestQueueImp_DeQueue(t *testing.T) {
	t.Run("empty queue", func(t *testing.T) {
		q := queueImp[int]{}
		_, ok := q.Dequeue()
		if ok {
			t.Errorf("expected false on empty queue")
		}
	})
	t.Run("non empty queue", func(t *testing.T) {
		q := queueImp[int]{}
		q.Enqueue(1)
		q.Enqueue(2)
		val, ok := q.Dequeue()
		if !ok {
			t.Errorf("expected false on non empty queue")
		}
		if q.Len() != 1 {
			t.Errorf("expected len = 1 got: %d", q.Len())
		}
		if val != 1 {
			t.Errorf("expected 1 on val got: %d", val)
		}
	})
}

func TestQueueImp_Peek(t *testing.T) {
	t.Run("empty queue", func(t *testing.T) {
		q := queueImp[int]{}
		_, ok := q.Peek()
		if ok {
			t.Errorf("expected false on empty queue")
		}
	})
	t.Run("non empty queue", func(t *testing.T) {
		q := queueImp[int]{}
		q.Enqueue(1)
		q.Enqueue(2)
		val, ok := q.Peek()
		if !ok {
			t.Errorf("expected false on non empty queue")
		}
		if q.Len() != 2 {
			t.Errorf("expected len = 2 got: %d", q.Len())
		}
		if val != 1 {
			t.Errorf("expected 1 on val got: %d", val)
		}
	})
}
