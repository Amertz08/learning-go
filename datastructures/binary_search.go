package datastructures

import (
	"golang.org/x/exp/constraints"
)

func BinarySearch[T constraints.Ordered](data []T, target T) int {
	left, right := 0, len(data)-1

	for left <= right {
		mid := (left + right) / 2

		guess := data[mid]
		if target < guess {
			right = mid - 1
		} else if target > guess {
			left = mid + 1
		} else {
			return mid
		}
	}
	return -1
}
