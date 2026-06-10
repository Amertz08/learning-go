package matrix_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.come/Amertz08/learning-go/datastructures"
)

func TestMatrix(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "binary search")
}

var _ = Describe("matrix testing", func() {
	When("empty", func() {
		It("will return 2 for rows", func() {
			m := datastructures.NewMatrix[int](2, 2)

			Expect(m.Rows()).To(Equal(2))
		})
		It("will return 2 for columns", func() {
			m := datastructures.NewMatrix[int](2, 2)

			Expect(m.Columns()).To(Equal(2))
		})
		DescribeTable("get will return an error if out of bounds", func(row, col int) {
			m := datastructures.NewMatrix[int](2, 2)
			_, err := m.Get(row, col)
			Expect(err).To(HaveOccurred())
		},
			Entry("negative row", -1, 1),
			Entry("negative col", 1, -1),
			Entry("too big of row", 11, 1),
			Entry("too big of col", 1, 11),
		)
		It("will return a value", func() {
			m := datastructures.NewMatrix[int](2, 2)
			val, _ := m.Get(1, 1)
			Expect(val).To(Equal(0))
		})
		It("can set the value", func() {
			m := datastructures.NewMatrix[int](2, 2)

			newVal := 6

			err := m.Set(1, 1, newVal)
			Expect(err).ToNot(HaveOccurred())

			val, _ := m.Get(1, 1)
			Expect(val).To(Equal(newVal))
		})

		DescribeTable("set will return an error if out of bounds", func(row, col int) {
			m := datastructures.NewMatrix[int](2, 2)
			err := m.Set(row, col, 6)
			Expect(err).To(HaveOccurred())
		},
			Entry("negative row", -1, 1),
			Entry("negative col", 1, -1),
			Entry("too big of row", 11, 1),
			Entry("too big of col", 1, 11),
		)
	})
})
