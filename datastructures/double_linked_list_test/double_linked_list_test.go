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

var _ = Describe("Double linked list", func() {
	var uut *datastructures.DoubleLinkedList

	BeforeEach(func() {
		uut = datastructures.NewDoubleLinkedList()
	})

	When("list is empty", func() {
		It("will have a length of zero", func() {
			Expect(uut.Size()).To(Equal(0))
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
			Expect(uut.Size()).To(Equal(1))
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
			uut.PushEnd(initialValue)
		})

		It("will have a length of 1", func() {
			Expect(uut.Size()).To(Equal(1))
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
