package binary_search_tree

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.come/Amertz08/learning-go/datastructures"
)

var _ = Describe("min heap tests", func() {
	var heap datastructures.Heap[int]

	BeforeEach(func() {
		heap = datastructures.CompareHeap[int](func(x int, y int) bool {
			return x < y
		})
	})

	DescribeTable("adding to the heap", func(insertVals []int, expectedVals []int) {
		for _, v := range insertVals {
			heap.Add(v)
		}

		Expect(heap.List()).To(Equal(expectedVals))
	},
		Entry("empty list", []int{}, []int{}),
		Entry("single value", []int{1}, []int{1}),
		Entry("multiple values already in order", []int{1, 2}, []int{1, 2}),
		Entry("multiple values not in order", []int{2, 1}, []int{1, 2}),
		Entry("multiple values not in order case 2", []int{5, 2, 1}, []int{1, 5, 2}),
	)
	DescribeTable("popping from the heap", func(insertVals []int, expectedVal int, expectedVals []int) {
		for _, v := range insertVals {
			heap.Add(v)
		}

		val := heap.Pop()
		Expect(val).To(Equal(val))
		Expect(heap.List()).To(Equal(expectedVals))
	},
		Entry("empty list", []int{}, 0, []int{}),
		Entry("single value", []int{1}, 1, []int{}),
		Entry("multiple values already in order", []int{1, 2}, 1, []int{2}),
		Entry("multiple values not in order", []int{2, 1}, 1, []int{2}),
		Entry("multiple values not in order case 2", []int{5, 2, 1}, 1, []int{5, 2}),
	)
})
