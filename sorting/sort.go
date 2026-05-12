package sorting

type SortFunc func([]int) []int

func InsertionSort(data []int) []int {
	for curPtr := 1; curPtr < len(data); curPtr++ {
		leftPtr := curPtr - 1
		// while not at front of list AND the values we're looking at are less than each other
		for leftPtr >= 0 && data[leftPtr+1] < data[leftPtr] {
			// swap spots and move leftPtr towards the front
			tmp := data[leftPtr+1]
			data[leftPtr+1] = data[leftPtr]
			data[leftPtr] = tmp
			leftPtr--
		}

	}
	return data
}

func MergeSort(data []int) []int {

	if len(data) <= 1 {
		return data
	}

	helper := make([]int, len(data))

	copy(helper, data)

	mergeSort(helper, 0, len(helper)-1)

	return helper

}

func mergeSort(data []int, s, e int) {
	if s >= e {
		return
	}

	m := (s + e) / 2
	mergeSort(data, s, m)
	mergeSort(data, m+1, e)

	merge(data, s, m, e)

}

func merge(data []int, s, m, e int) {
	left := append([]int{}, data[s:m+1]...)
	right := append([]int{}, data[m+1:e+1]...)

	i, j, k := 0, 0, s

	for i < len(left) && j < len(right) {
		if left[i] <= right[j] {
			data[k] = left[i]
			i++
		} else {
			data[k] = right[j]
			j++
		}
		k++
	}

	for i < len(left) {
		data[k] = left[i]
		i++
		k++
	}
	for j < len(right) {
		data[k] = right[j]
		j++
		k++
	}
}
