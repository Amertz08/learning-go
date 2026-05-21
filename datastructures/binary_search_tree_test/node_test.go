package binary_search_tree_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.come/Amertz08/learning-go/datastructures"
)

var _ = Describe("interacting with BST node", func() {
	When("it has a value", func() {
		It("can return the value", func() {
			val := 1
			n := datastructures.NewBSTNodeImpl[int](val)

			Expect(n.Value()).To(Equal(val))
		})
		It("will return false when accessing left", func() {
			val := 1
			n := datastructures.NewBSTNodeImpl[int](val)

			_, ok := n.Left()
			Expect(ok).To(BeFalse())
		})
		It("will return false when accessing right", func() {
			val := 1
			n := datastructures.NewBSTNodeImpl[int](val)

			_, ok := n.Right()
			Expect(ok).To(BeFalse())
		})
		It("will return false when using HasLeft", func() {
			val := 1
			n := datastructures.NewBSTNodeImpl[int](val)

			Expect(n.HasLeft()).To(BeFalse())
		})
		It("will return false when using HasRight", func() {
			val := 1
			n := datastructures.NewBSTNodeImpl[int](val)

			Expect(n.HasRight()).To(BeFalse())
		})
	})
})
