package datastructures

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestLinkedList(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Linked List")
}

var _ = Describe("We are interacting with a linked list", func() {
	var uut LinkedList[int]
	var zeroVal int

	BeforeEach(func() {
		uut = NewLinkedList[int]()
	})

	When("the list is empty", func() {
		It("will tell you it is empty", func() {
			Expect(uut.IsEmpty()).To(BeTrue())
		})
		It("will have a length of zero", func() {
			Expect(uut.Len()).To(Equal(0))
		})
		It("will no be able to correctly return the front node", func() {
			_, ok := uut.Front()
			Expect(ok).To(BeFalse())
		})
		It("will return a nil value for the front node", func() {
			val, _ := uut.Front()
			Expect(val).To(BeNil())
		})
		It("will not be able to correctly return the back node", func() {
			_, ok := uut.Back()
			Expect(ok).To(BeFalse())
		})
		It("will return a nil value for the back node", func() {
			val, _ := uut.Back()
			Expect(val).To(BeNil())
		})
		It("will not be able to remove the front value", func() {
			_, ok := uut.PopFront()
			Expect(ok).To(BeFalse())
		})
		It("will return a zero value for the front", func() {
			val, _ := uut.PopFront()
			Expect(val).To(Equal(zeroVal))
		})
		It("will not be able to return the back value", func() {
			_, ok := uut.PopBack()
			Expect(ok).To(BeFalse())
		})
		It("will return a zero value for the back", func() {
			val, _ := uut.PopBack()
			Expect(val).To(Equal(zeroVal))
		})
	})
	When("a value was added to the front", func() {
		pushedValue := 1

		BeforeEach(func() {
			uut.PushFront(pushedValue)
		})
		It("will no longer be empty", func() {
			Expect(uut.IsEmpty()).To(BeFalse())
		})
		It("will have a length of one", func() {
			Expect(uut.Len()).To(Equal(1))
		})
		It("will not have an issue returning the value from the front", func() {
			_, ok := uut.Front()
			Expect(ok).To(BeTrue())
		})
		It("will assign the value to the front", func() {
			observed, _ := uut.Front()
			Expect(observed.Value()).To(Equal(pushedValue))
		})
		It("will not have an issue returning the value from the back", func() {
			_, ok := uut.Back()
			Expect(ok).To(BeTrue())
		})
		It("will assign the value to the back", func() {
			observed, _ := uut.Back()
			Expect(observed.Value()).To(Equal(pushedValue))
		})
		It("has back and front as the same node", func() {
			observedBack, _ := uut.Back()
			observedFront, _ := uut.Front()
			Expect(observedFront).To(BeIdenticalTo(observedBack))
		})
		It("can remove the value from the front", func() {
			_, ok := uut.PopFront()
			Expect(ok).To(BeTrue())
		})
		It("will return the value from the front when removed", func() {
			val, _ := uut.PopFront()
			Expect(val).To(Equal(pushedValue))
		})
		It("can remove the value from the back", func() {
			_, ok := uut.PopBack()
			Expect(ok).To(BeTrue())
		})
		It("will return the value from the back", func() {
			val, _ := uut.PopBack()
			Expect(val).To(Equal(pushedValue))
		})
	})
	When("a value is added to the back", func() {
		pushedValue := 1

		BeforeEach(func() {
			uut.PushBack(pushedValue)
		})
		It("will no longer be empty", func() {
			Expect(uut.IsEmpty()).To(BeFalse())
		})
		It("will have a length of one", func() {
			Expect(uut.Len()).To(Equal(1))
		})
		It("will not have an issue returning the value from the front", func() {
			_, ok := uut.Front()
			Expect(ok).To(BeTrue())
		})
		It("will assign the value to the front", func() {
			observed, _ := uut.Front()
			Expect(observed.Value()).To(Equal(pushedValue))
		})
		It("will not have an issue returning the value from the back", func() {
			_, ok := uut.Back()
			Expect(ok).To(BeTrue())
		})
		It("will assign the value to the back", func() {
			observed, _ := uut.Back()
			Expect(observed.Value()).To(Equal(pushedValue))
		})
		It("has back and front as the same node", func() {
			observedBack, _ := uut.Back()
			observedFront, _ := uut.Front()
			Expect(observedFront).To(BeIdenticalTo(observedBack))
		})
		It("can remove the value from the front", func() {
			_, ok := uut.PopFront()
			Expect(ok).To(BeTrue())
		})
		It("will return the value from the front when removed", func() {
			val, _ := uut.PopFront()
			Expect(val).To(Equal(pushedValue))
		})
		It("can remove the value from the back", func() {
			_, ok := uut.PopBack()
			Expect(ok).To(BeTrue())
		})
		It("will return the value from the back", func() {
			val, _ := uut.PopBack()
			Expect(val).To(Equal(pushedValue))
		})
	})
	When("there is already an item in the list", func() {
		initialValue := 1

		BeforeEach(func() {
			uut.PushFront(initialValue)
		})
		When("another item is pushed to the front", func() {
			secondValue := 2

			BeforeEach(func() {
				uut.PushFront(secondValue)
			})

			It("will have a length of 2", func() {
				Expect(uut.Len()).To(Equal(2))
			})
			It("will return the new item as the front", func() {
				front, _ := uut.Front()
				Expect(front.Value()).To(Equal(secondValue))
			})
			It("will return the initial value as the new back", func() {
				back, _ := uut.Back()
				Expect(back.Value()).To(Equal(initialValue))
			})
			It("can successfully remove the value from the front", func() {
				_, ok := uut.PopFront()
				Expect(ok).To(BeTrue())
			})
			It("will remove the correct value from the front", func() {
				val, _ := uut.PopFront()
				Expect(val).To(Equal(secondValue))
			})
			It("can successfully remove the value from the back", func() {
				_, ok := uut.PopBack()
				Expect(ok).To(BeTrue())
			})
			It("will remove the correct value from the back", func() {
				val, _ := uut.PopBack()
				Expect(val).To(Equal(initialValue))
			})
		})
		When("another item is pushed to the back", func() {
			secondValue := 2

			BeforeEach(func() {
				uut.PushBack(secondValue)
			})

			It("will have a length of 2", func() {
				Expect(uut.Len()).To(Equal(2))
			})
			It("will return the initial item as the front", func() {
				front, _ := uut.Front()
				Expect(front.Value()).To(Equal(initialValue))
			})
			It("will return the second value as the new back", func() {
				back, _ := uut.Back()
				Expect(back.Value()).To(Equal(secondValue))
			})
			It("can successfully remove the value from the front", func() {
				_, ok := uut.PopFront()
				Expect(ok).To(BeTrue())
			})
			It("will remove the correct value from the front", func() {
				val, _ := uut.PopFront()
				Expect(val).To(Equal(initialValue))
			})
			It("can successfully remove the value from the back", func() {
				_, ok := uut.PopBack()
				Expect(ok).To(BeTrue())
			})
			It("will remove the correct value from the back", func() {
				val, _ := uut.PopBack()
				Expect(val).To(Equal(secondValue))
			})
		})
	})
})
