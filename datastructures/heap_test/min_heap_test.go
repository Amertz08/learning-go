package binary_search_tree

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.come/Amertz08/learning-go/datastructures"
)

var _ = Describe("min heap tests", func() {
	var heap datastructures.Heap[int]

	BeforeEach(func() {
		heap = datastructures.NewMinHeap[int]()
	})

	DescribeTable("adding to the heap", func(insertVals []int, expectedVals []int) {
		for _, v := range insertVals {
			heap.Add(v)
		}

		Expect(heap.List()).To(Equal(expectedVals))
	},
		Entry("empty list", []int{}, []int{}),
	)
})
