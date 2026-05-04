package stack_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.come/Amertz08/learning-go/datastructures"
)

func TestStack(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Stack Suite")
}

var _ = Describe("Stack", func() {
	var uut datastructures.Stack[int]
	BeforeEach(func() {
		uut = datastructures.NewStack[int]()
	})
	When("the stack is empty", func() {
		It("should have a length of 0", func() {
			Expect(uut.Len()).To(Equal(0))
		})
		It("should tell you it's empty", func() {
			Expect(uut.IsEmpty()).To(BeTrue())
		})
		It("should not be able to return the top value", func() {
			val, ok := uut.Peek()
			Expect(ok).To(BeFalse())
			Expect(val).To(Equal(0))
		})
		It("can push an item", func() {
			value := 1
			uut.Push(value)
			obs, ok := uut.Peek()
			Expect(ok).To(BeTrue())
			Expect(obs).To(Equal(value))
		})
		It("will not return the value from popping", func() {
			_, ok := uut.Pop()
			Expect(ok).To(BeFalse())
		})
	})
	When("it already has an item", func() {
		var value int
		BeforeEach(func() {
			value = 1
			uut.Push(value)
		})
		It("should have a length of 1", func() {
			Expect(uut.Len()).To(Equal(1))
		})
		It("should not be empty", func() {
			Expect(uut.IsEmpty()).To(BeFalse())
		})
		It("can peek the value", func() {
			val, ok := uut.Peek()
			Expect(ok).To(BeTrue())
			Expect(val).To(Equal(value))
		})
		It("can remove the item", func() {
			val, ok := uut.Pop()
			Expect(ok).To(BeTrue())
			Expect(val).To(Equal(value))
		})
		When("another value is pushed into the stack", func() {
			var secondValue int
			BeforeEach(func() {
				secondValue = 5
				uut.Push(secondValue)
			})
			It("should have a length of 2", func() {
				Expect(uut.Len()).To(Equal(2))
			})
			It("should peek the new value", func() {
				val, ok := uut.Peek()
				Expect(ok).To(BeTrue())
				Expect(val).To(Equal(secondValue))
			})
			It("will return the value from popping", func() {
				val, ok := uut.Pop()
				Expect(ok).To(BeTrue())
				Expect(val).To(Equal(secondValue))
			})
		})
	})
	When("there are multiple items", func() {
		var firstValue, secondValue int
		BeforeEach(func() {
			firstValue = 1
			secondValue = 2
			uut.Push(firstValue)
			uut.Push(secondValue)
		})
		It("will pop values in order", func() {
			val, ok := uut.Pop()
			Expect(ok).To(BeTrue())
			Expect(val).To(Equal(secondValue))
			val, ok = uut.Pop()
			Expect(ok).To(BeTrue())
			Expect(val).To(Equal(firstValue))
		})
	})
})
