package queue_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.come/Amertz08/learning-go/datastructures"
)

func TestLinkedList(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Queue")
}

var _ = Describe("Interacting with a queue", func() {
	var uut datastructures.Queue[int]
	var zeroVal int

	BeforeEach(func() {
		// TODO: swap between types
		uut = datastructures.NewSliceQueue[int]()
	})

	When("the queue is empty", func() {
		It("will have a length of zero", func() {
			Expect(uut.Len()).To(Equal(0))
		})
		It("will tell you it is empty", func() {
			Expect(uut.IsEmpty()).To(BeTrue())
		})
		It("will error when you peek the front", func() {
			_, ok := uut.Peek()
			Expect(ok).To(BeFalse())
		})
		It("will return a zero value when the front is peeked", func() {
			val, _ := uut.Peek()
			Expect(val).To(Equal(zeroVal))
		})
		It("will error if a value is dequeued", func() {
			_, ok := uut.Dequeue()
			Expect(ok).To(BeFalse())
		})
		It("will return a zero value of the item if dequeued", func() {
			val, _ := uut.Dequeue()
			Expect(val).To(Equal(zeroVal))
		})
	})
})

func TestQueue_Len(t *testing.T) {
	t.Run("queue with one item", func(t *testing.T) {
		q := datastructures.NewSliceQueue[int]()

		q.Enqueue(1)

		if q.Len() != 1 {
			t.Errorf("expected 1 on queue got: %d", q.Len())
		}
	})
}

func TestQueue_IsEmpty(t *testing.T) {
	t.Run("non empty queue", func(t *testing.T) {
		q := datastructures.NewSliceQueue[int]()

		q.Enqueue(1)
		ok := q.IsEmpty()
		if ok {
			t.Errorf("expected false got true")
		}
	})
}

func TestQueueImp_DeQueue(t *testing.T) {
	t.Run("non empty queue", func(t *testing.T) {
		q := datastructures.NewSliceQueue[int]()
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
	t.Run("non empty queue", func(t *testing.T) {
		q := datastructures.NewSliceQueue[int]()
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
