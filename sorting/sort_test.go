package sorting

import (
	"slices"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestSorting(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "sorting tests")
}

type SortingTestCase struct {
	Name     string
	Input    []int
	Expected []int
}

var sortingTestCases = []SortingTestCase{
	{
		Name:     "empty",
		Input:    []int{},
		Expected: []int{},
	},
	{
		Name:     "single element",
		Input:    []int{1},
		Expected: []int{1},
	},
	{
		Name:     "already sorted",
		Input:    []int{1, 2, 3, 4, 5},
		Expected: []int{1, 2, 3, 4, 5},
	},
	{
		Name:     "reverse sorted",
		Input:    []int{5, 4, 3, 2, 1},
		Expected: []int{1, 2, 3, 4, 5},
	},
	{
		Name:     "two elements",
		Input:    []int{2, 1},
		Expected: []int{1, 2},
	},
	{
		Name:     "duplicates",
		Input:    []int{3, 1, 2, 3, 1, 2},
		Expected: []int{1, 1, 2, 2, 3, 3},
	},
	{
		Name:     "all duplicates",
		Input:    []int{7, 7, 7, 7, 7},
		Expected: []int{7, 7, 7, 7, 7},
	},
	{
		Name:     "negative numbers",
		Input:    []int{-3, -1, -7, -2, -5},
		Expected: []int{-7, -5, -3, -2, -1},
	},
	{
		Name:     "mixed positive and negative",
		Input:    []int{-10, 5, 0, -3, 8, -1},
		Expected: []int{-10, -3, -1, 0, 5, 8},
	},
	{
		Name:     "includes zero",
		Input:    []int{0, 5, 2, 0, -1, 3},
		Expected: []int{-1, 0, 0, 2, 3, 5},
	},
	{
		Name:     "large values",
		Input:    []int{1000000, -1000000, 999999, -999999},
		Expected: []int{-1000000, -999999, 999999, 1000000},
	},
	{
		Name:     "odd length",
		Input:    []int{9, 3, 7, 1, 5},
		Expected: []int{1, 3, 5, 7, 9},
	},
	{
		Name:     "even length",
		Input:    []int{8, 4, 6, 2},
		Expected: []int{2, 4, 6, 8},
	},
	{
		Name:     "nearly sorted",
		Input:    []int{1, 2, 3, 5, 4, 6, 7},
		Expected: []int{1, 2, 3, 4, 5, 6, 7},
	},
	{
		Name:     "random order",
		Input:    []int{42, 17, 8, 99, 23, 4, 16},
		Expected: []int{4, 8, 16, 17, 23, 42, 99},
	},
	{
		Name:     "repeated pattern",
		Input:    []int{1, 2, 1, 2, 1, 2, 1, 2},
		Expected: []int{1, 1, 1, 1, 2, 2, 2, 2},
	},
	{
		Name:     "large descending",
		Input:    []int{10, 9, 8, 7, 6, 5, 4, 3, 2, 1},
		Expected: []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
	},
	{
		Name:     "large ascending",
		Input:    []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
		Expected: []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
	},
	{
		Name:     "extremes",
		Input:    []int{2147483647, -2147483648, 0, 1, -1},
		Expected: []int{-2147483648, -1, 0, 1, 2147483647},
	},
	{
		Name:     "long random sample",
		Input:    []int{12, 4, 56, 17, 8, 99, 3, 45, 23, 67, 1, 88},
		Expected: []int{1, 3, 4, 8, 12, 17, 23, 45, 56, 67, 88, 99},
	},
}

type NamedSortFunc struct {
	Name string
	Fn   SortFunc
}

var sortingFuncs = []NamedSortFunc{
	{"InsertionSort", InsertionSort},
	{"MergeSort", MergeSort},
}

var _ = Describe("Sort", func() {
	for _, sortFunc := range sortingFuncs {
		sortFn := sortFunc.Fn

		Describe(sortFunc.Name, func() {
			var entries []TableEntry

			for _, tc := range sortingTestCases {
				entries = append(entries, Entry(tc.Name, tc))
			}

			DescribeTable("sorting arrays",
				func(tc SortingTestCase) {
					input := slices.Clone(tc.Input)

					result := sortFn(input)

					Expect(result).To(Equal(tc.Expected))
				},
				entries,
			)
		})
	}
})
