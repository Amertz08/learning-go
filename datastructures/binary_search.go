package datastructures

import (
	"golang.org/x/exp/constraints"
)

func BinarySearch[T constraints.Ordered](data []T, target T) int {
	left, right := 0, len(data)-1

	for left <= right {
		mid := (left + right) / 2

		guess := data[mid]
		if guess == target {
			return mid
		} else if target < guess {
			right = mid - 1
		} else {
			left = mid + 1
		}
	}
	return -1
}
