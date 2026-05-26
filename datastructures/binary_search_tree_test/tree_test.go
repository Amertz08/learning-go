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
		It("will return false for min", func() {
			_, ok := uut.Min()
			Expect(ok).To(BeFalse())
		})
		It("will return false for max", func() {
			_, ok := uut.Max()
			Expect(ok).To(BeFalse())
		})
		It("will return false for root", func() {
			_, ok := uut.Root()
			Expect(ok).To(BeFalse())
		})
		It("will return nil for the root node", func() {
			n, _ := uut.Root()
			Expect(n).To(BeNil())
		})
		It("will return false for find", func() {
			_, ok := uut.Find(2)
			Expect(ok).To(BeFalse())
		})
		It("will return nil for find answer", func() {
			n, _ := uut.Find(2)
			Expect(n).To(BeNil())
		})
		It("will return false for contains", func() {
			Expect(uut.Contains(2)).To(BeFalse())
		})
		It("will return false for remove", func() {
			Expect(uut.Remove(2)).To(BeFalse())
		})
		It("will return nil for InOrder", func() {
			Expect(uut.InOrder()).To(BeNil())
		})
		It("will return nil for PreOrder", func() {
			Expect(uut.PreOrder()).To(BeNil())
		})
		It("will return nil for PostOrder", func() {
			Expect(uut.PostOrder()).To(BeNil())
		})
		It("will return nil for LevelOrder", func() {
			Expect(uut.LevelOrder()).To(BeNil())
		})
	})

	When("inserting on an empty tree", func() {
		var insertedVal int
		var didInsert bool

		BeforeEach(func() {
			insertedVal = 1
			didInsert = uut.Insert(insertedVal)
		})
		It("returns true", func() {
			Expect(didInsert).To(BeTrue())
		})
		It("will increase the length", func() {
			Expect(uut.Len()).To(Equal(1))
		})
		It("will no longer be empty", func() {
			Expect(uut.IsEmpty()).To(BeFalse())
		})
		It("contains the value", func() {
			Expect(uut.Contains(insertedVal)).To(BeTrue())
		})
		It("can find the node", func() {
			_, ok := uut.Find(insertedVal)
			Expect(ok).To(BeTrue())
		})
		It("will return the found node", func() {
			n, _ := uut.Find(insertedVal)
			Expect(n).ToNot(BeNil())
			Expect(n.Value()).To(Equal(insertedVal))
		})
		It("can find root", func() {
			_, ok := uut.Root()
			Expect(ok).To(BeTrue())
		})
		It("will return the root", func() {
			n, _ := uut.Root()
			Expect(n).ToNot(BeNil())
			Expect(n.Value()).To(Equal(insertedVal))
		})
		It("can find min", func() {
			_, ok := uut.Min()
			Expect(ok).To(BeTrue())
		})
		It("will return the value as min", func() {
			val, _ := uut.Min()
			Expect(val).To(Equal(insertedVal))
		})
		It("can find max", func() {
			_, ok := uut.Max()
			Expect(ok).To(BeTrue())
		})
		It("will return the value as Max", func() {
			val, _ := uut.Max()
			Expect(val).To(Equal(insertedVal))
		})
		It("will return the tree in order", func() {
			tree := uut.InOrder()
			Expect(tree).To(Equal([]int{insertedVal}))
		})
		It("will return the tree in preorder", func() {
			tree := uut.PreOrder()
			Expect(tree).To(Equal([]int{insertedVal}))
		})
		It("will return the tree in postorder", func() {
			tree := uut.PostOrder()
			Expect(tree).To(Equal([]int{insertedVal}))
		})
		It("will return the tree in level order", func() {
			tree := uut.LevelOrder()
			Expect(tree).To(Equal([]int{insertedVal}))
		})
		It("will not insert the same value twice", func() {
			Expect(uut.Insert(insertedVal)).To(BeFalse())
		})
	})
	When("inserting values", func() {
		It("can insert a single value", func() {
			value := 1
			isOk := uut.Insert(value)
			Expect(isOk).To(BeTrue())
			Expect(uut.InOrder()).To(Equal([]int{value}))
		})
		It("can insert a multiple values", func() {
			firstValue := 1
			secondValue := 2
			isOk := uut.Insert(firstValue)
			Expect(isOk).To(BeTrue())

			isOk = uut.Insert(secondValue)
			Expect(uut.InOrder()).To(Equal([]int{firstValue, secondValue}))
		})
		It("can insert a multiple values2", func() {
			firstValue := 1
			secondValue := 2
			isOk := uut.Insert(secondValue)
			Expect(isOk).To(BeTrue())

			isOk = uut.Insert(firstValue)
			Expect(isOk).To(BeTrue())
			Expect(uut.InOrder()).To(Equal([]int{firstValue, secondValue}))
		})
		It("can insert a multiple values3", func() {
			firstValue := 1
			secondValue := 2
			thirdValue := -1
			isOk := uut.Insert(secondValue)
			Expect(isOk).To(BeTrue())

			isOk = uut.Insert(firstValue)
			Expect(isOk).To(BeTrue())

			isOk = uut.Insert(thirdValue)
			Expect(isOk).To(BeTrue())
			Expect(uut.InOrder()).To(Equal([]int{thirdValue, firstValue, secondValue}))
		})
	})
})
