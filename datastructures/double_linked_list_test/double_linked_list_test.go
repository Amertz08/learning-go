package double_linked_list_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.come/Amertz08/learning-go/datastructures"
)

func TestDoubleLinkedList(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Double Linked List")
}

var _ = Describe("Generic Double Linked List", func() {
	var uut datastructures.DoublyLinkedList[int]
	var zeroVal int

	BeforeEach(func() {
		uut = datastructures.NewGenericDoubleLinkedList[int]()
	})

	When("list is empty", func() {
		It("will have a length of zero", func() {
			Expect(uut.Len()).To(Equal(0))
		})
		It("will tell you it is empty", func() {
			Expect(uut.IsEmpty()).To(BeTrue())
		})
		It("will fail accessing value from the front", func() {
			_, ok := uut.Front()
			Expect(ok).To(BeFalse())
		})
		It("will return a nil from the front", func() {
			front, _ := uut.Front()
			Expect(front).To(BeNil())
		})
		It("will fail accessing value from the back", func() {
			_, ok := uut.Back()
			Expect(ok).To(BeFalse())
		})
		It("will return a nil from the back", func() {
			back, _ := uut.Back()
			Expect(back).To(BeNil())
		})
		It("will fail to pop from the front", func() {
			_, ok := uut.PopFront()
			Expect(ok).To(BeFalse())
		})
		It("will return zero value when trying to pop the front", func() {
			val, _ := uut.PopFront()
			Expect(val).To(Equal(zeroVal))
		})
		It("will fail to pop from the back", func() {
			_, ok := uut.PopBack()
			Expect(ok).To(BeFalse())
		})
		It("will return zero value when trying to pop the back", func() {
			val, _ := uut.PopBack()
			Expect(val).To(Equal(zeroVal))
		})
	})
	When("a value is pushed to the front", func() {
		var initialVal int

		BeforeEach(func() {
			initialVal = 5
			uut.PushFront(initialVal)
		})

		It("will have a length of 1", func() {
			Expect(uut.Len()).To(Equal(1))
		})
		It("will not be empty", func() {
			Expect(uut.IsEmpty()).To(BeFalse())
		})
		It("will assign the value to the front", func() {
			front, _ := uut.Front()

			Expect(front).ToNot(BeNil())
			Expect(front.Value()).To(Equal(initialVal))
		})
		It("will assign the value to the back", func() {
			back, _ := uut.Back()

			Expect(back).ToNot(BeNil())
			Expect(back.Value()).To(Equal(initialVal))
		})
	})
	When("there is already one item in the list", func() {
		var firstValue int

		BeforeEach(func() {
			firstValue = 5
			uut.PushFront(firstValue)
		})
		When("a second value is pushed to the front", func() {
			var secondValue int

			BeforeEach(func() {
				secondValue = 2
				uut.PushFront(secondValue)
			})
			It("will have a length of 2", func() {
				Expect(uut.Len()).To(Equal(2))
			})
			It("will return the new item as the front", func() {
				front, _ := uut.Front()
				Expect(front.Value()).To(Equal(secondValue))
			})
		})
	})
})

var _ = Describe("Double linked list", func() {
	var uut *datastructures.DoubleLinkedList

	BeforeEach(func() {
		uut = datastructures.NewDoubleLinkedList()
	})

	When("list is empty", func() {
		It("will have a length of zero", func() {
			Expect(uut.Len()).To(Equal(0))
		})
		It("will tell you it is empty", func() {
			Expect(uut.IsEmpty()).To(BeTrue())
		})
		It("will return nil for the front", func() {
			Expect(uut.Front()).To(BeNil())
		})
		It("will return nil for the back", func() {
			Expect(uut.Back()).To(BeNil())
		})
		It("will return an error when you attempt to remove a value", func() {
			Expect(uut.Remove(2)).Should(HaveOccurred())
		})
	})
	When("a single value is pushed to the front", func() {
		var initialValue int

		BeforeEach(func() {
			initialValue = 2
			uut.PushFront(initialValue)
		})

		It("will have a length of 1", func() {
			Expect(uut.Len()).To(Equal(1))
		})
		It("will tell you it is not empty", func() {
			Expect(uut.IsEmpty()).To(BeFalse())
		})
		It("will return the value from the front", func() {
			Expect(uut.Front()).ToNot(BeNil())
			Expect(uut.Front().Val).To(Equal(initialValue))
		})
		It("will return the value from the back", func() {
			Expect(uut.Back()).ToNot(BeNil())
			Expect(uut.Back().Val).To(Equal(initialValue))
		})
		It("will not return an error when you remove the value", func() {
			Expect(uut.Remove(initialValue)).ShouldNot(HaveOccurred())
		})
	})
	When("a single value is pushed to the back", func() {
		var initialValue int

		BeforeEach(func() {
			initialValue = 2
			uut.PushBack(initialValue)
		})

		It("will have a length of 1", func() {
			Expect(uut.Len()).To(Equal(1))
		})
		It("will tell you it is not empty", func() {
			Expect(uut.IsEmpty()).To(BeFalse())
		})
		It("will return the value from the front", func() {
			Expect(uut.Front()).ToNot(BeNil())
			Expect(uut.Front().Val).To(Equal(initialValue))
		})
		It("will return the value from the back", func() {
			Expect(uut.Back()).ToNot(BeNil())
			Expect(uut.Back().Val).To(Equal(initialValue))
		})
		It("will not return an error when you remove the value", func() {
			Expect(uut.Remove(initialValue)).ShouldNot(HaveOccurred())
		})
	})
})
