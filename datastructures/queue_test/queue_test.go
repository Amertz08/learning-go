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

	When("a single value is in the queue", func() {
		var initialValue int
		BeforeEach(func() {
			initialValue = 2
			uut.Enqueue(initialValue)
		})

		It("will have a length of 1", func() {
			Expect(uut.Len()).To(Equal(1))
		})
		It("will tell you it is not empty", func() {
			Expect(uut.IsEmpty()).To(BeFalse())
		})
		It("can show you the front value", func() {
			_, ok := uut.Peek()
			Expect(ok).To(BeTrue())
		})
		It("will set the value at the front", func() {
			val, _ := uut.Peek()
			Expect(val).To(Equal(initialValue))
		})
		It("can remove the value", func() {
			_, ok := uut.Dequeue()
			Expect(ok).To(BeTrue())
		})
		It("will return the actual value", func() {
			val, _ := uut.Dequeue()
			Expect(val).To(Equal(initialValue))
		})
		When("a second value is added to the queue", func() {
			var secondValue int

			BeforeEach(func() {
				secondValue = 5
				uut.Enqueue(secondValue)
			})
			It("will have a length of 2", func() {
				Expect(uut.Len()).To(Equal(2))
			})
			It("will remove the values in order", func() {
				firstObserved, _ := uut.Dequeue()
				secondObserved, _ := uut.Dequeue()
				Expect(firstObserved).To(Equal(initialValue))
				Expect(secondObserved).To(Equal(secondValue))
			})
		})
	})
})
