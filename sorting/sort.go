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
