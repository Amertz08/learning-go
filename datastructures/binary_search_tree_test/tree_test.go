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

	DescribeTable("Inserting values valid cases", func(insertVal []int, expected []int) {
		for _, val := range insertVal {
			Expect(uut.Insert(val)).To(BeTrue())
		}

		Expect(uut.InOrder()).To(Equal(expected))
		Expect(uut.Len()).To(Equal(len(expected)))
	},
		Entry("basic case", []int{1}, []int{1}),
		Entry("already in order", []int{1, 2}, []int{1, 2}),
		Entry("largest first", []int{2, 1}, []int{1, 2}),
		Entry("basic 3", []int{1, 2, 3}, []int{1, 2, 3}),
		Entry("negative", []int{1, 2, -1}, []int{-1, 1, 2}),
	)
	DescribeTable("Contains value cases", func(insertVal []int, target int, expected bool) {
		for _, val := range insertVal {
			Expect(uut.Insert(val)).To(BeTrue())
		}

		Expect(uut.Contains(target)).To(Equal(expected))
	},
		Entry("empty tree", []int{}, 1, false),
		Entry("basic case contains", []int{1}, 1, true),
		Entry("basic case does not contains", []int{1}, 2, false),
		Entry("exists in right tree", []int{1, 2}, 2, true),
		Entry("does not exist in right tree", []int{1, 2}, 3, false),
	)
	DescribeTable("Find value cases", func(insertVal []int, target int, expected bool) {
		for _, val := range insertVal {
			Expect(uut.Insert(val)).To(BeTrue())
		}

		obs, ok := uut.Find(target)
		Expect(ok).To(Equal(expected))
		if expected {
			Expect(obs.Value()).To(Equal(target))
		} else {
			Expect(obs).To(BeNil())
		}

	},
		Entry("empty tree", []int{}, 1, false),
		Entry("basic case contains", []int{1}, 1, true),
		Entry("basic case does not contains", []int{1}, 2, false),
		Entry("exists in right tree", []int{1, 2}, 2, true),
		Entry("does not exist in right tree", []int{1, 2}, 3, false),
	)
	DescribeTable("min value test cases", func(insertVals []int, target int, expected bool) {
		for _, val := range insertVals {
			Expect(uut.Insert(val)).To(BeTrue())
		}
		minVal, found := uut.Min()
		Expect(found).To(Equal(expected))
		Expect(minVal).To(Equal(target))
	},
		Entry("empty tree", []int{}, 0, false),
		Entry("single value", []int{1}, 1, true),
		Entry("value left", []int{1, -1}, -1, true),
		Entry("value right of left", []int{1, -5, -1}, -5, true),
		Entry("value right of left", []int{1, -1, -5}, -5, true),
	)
	DescribeTable("max value test cases", func(insertVals []int, target int, expected bool) {
		for _, val := range insertVals {
			Expect(uut.Insert(val)).To(BeTrue())
		}
		maxVal, found := uut.Max()
		Expect(found).To(Equal(expected))
		Expect(maxVal).To(Equal(target))
	},
		Entry("empty tree", []int{}, 0, false),
		Entry("single value", []int{1}, 1, true),
		Entry("still at root", []int{1, -1}, 1, true),
		Entry("value right", []int{1, 5, -1}, 5, true),
		Entry("value right of right", []int{1, 2, 5}, 5, true),
		Entry("value right with a left", []int{1, 3, 2, 5}, 5, true),
	)
	DescribeTable("remove test cases", func(insertVals []int, target int, expected bool, inOrder []int) {
		for _, val := range insertVals {
			Expect(uut.Insert(val)).To(BeTrue())
		}
		Expect(uut.Remove(target)).To(Equal(expected))
		Expect(uut.InOrder()).To(Equal(inOrder))
	},
		Entry("empty tree", []int{}, 1, false, nil),
		Entry("single value at root", []int{1}, 1, true, nil),
		Entry("single value not in tree", []int{1}, 2, false, []int{1}),
		Entry("delete value at root", []int{1, 5}, 1, true, []int{5}),
		Entry("delete value in right", []int{1, 5}, 5, true, []int{1}),
		Entry("delete value in left", []int{5, 1}, 1, true, []int{5}),
		Entry("delete middle node on right", []int{1, 5, 3}, 5, true, []int{1, 3}),
		Entry("delete middle node with left subtree that has a right", []int{1, 5, 3, 4}, 5, true, []int{1, 3, 4}),
		Entry("delete left subtree in right subtree", []int{1, 5, 3, 4}, 3, true, []int{1, 4, 5}),
	)
	DescribeTable("pre order traversal", func(insertVals []int, expected []int) {
		for _, val := range insertVals {
			Expect(uut.Insert(val)).To(BeTrue())
		}
		Expect(uut.PreOrder()).To(Equal(expected))
	},
		Entry("empty tree", []int{}, []int{}),
		Entry("single item", []int{1}, []int{1}),
		Entry("basic left tree", []int{5, 1}, []int{5, 1}),
		Entry("basic right tree", []int{1, 5}, []int{1, 5}),
		Entry("complex left tree", []int{5, 1, 3}, []int{5, 1, 3}),
		Entry("basic right tree", []int{1, 3, 5}, []int{1, 3, 5}),
	)
})
