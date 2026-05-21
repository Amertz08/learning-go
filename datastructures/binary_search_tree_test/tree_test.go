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

	When("inserting on an empty tree", func() {
		It("returns true", func() {
			Expect(uut.Insert(1)).To(BeTrue())
		})
	})
})
