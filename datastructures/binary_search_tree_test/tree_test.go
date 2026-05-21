package binary_search_tree

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.come/Amertz08/learning-go/datastructures"
)

var _ = Describe("interacting with BST", func() {
	var uut datastructures.BinarySearchTree[int]

	BeforeEach(func() {
		uut = datastructures.NewBSTImpl[int]()
	})

	When("interacting with an empty tree", func() {
		It("will have a length of zero", func() {
			Expect(uut.Len()).To(Equal(0))
		})
		It("will be empty", func() {
			Expect(uut.IsEmpty()).To(BeTrue())
		})
	})

	When("inserting on an empty tree", func() {
		It("returns true", func() {
			Expect(uut.Insert(1)).To(BeTrue())
		})
		It("will increase the length", func() {
			uut.Insert(1)

			Expect(uut.Len()).To(Equal(1))
		})
		It("will no longer be empty", func() {
			uut.Insert(1)

			Expect(uut.IsEmpty()).To(BeFalse())
		})
	})
})
