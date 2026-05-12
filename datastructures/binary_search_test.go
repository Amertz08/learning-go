package datastructures

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestBinarySearch(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "binary search")
}

var _ = Describe("using binary search", func() {
	When("value exists", func() {
		It("will find it", func() {
			input := []int{1, 2, 4, 6}
			Expect(BinarySearch[int](input, 4)).To(Equal(2))
		})
	})
	When("value does not exist", func() {
		It("will not find it", func() {
			input := []int{1, 2, 4, 6}
			Expect(BinarySearch[int](input, 5)).To(Equal(-1))
		})
	})
})
