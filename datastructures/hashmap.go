package datastructures

const defaultHashSize = 2

type HashMap[T any] interface {
	Put(key string, value T)
	Get(key string) (T, bool)
}

type hashValue[T any] struct {
	key   string
	value T
}

type basicHashImpl[T any] struct {
	data     []*hashValue[T]
	size     int
	capacity int
}

func NewHashMap[T any]() HashMap[T] {
	h := &basicHashImpl[T]{data: make([]*hashValue[T], defaultHashSize), size: 0}
	h.capacity = defaultHashSize
	return h
}

func (h *basicHashImpl[T]) Put(key string, value T) {
	idx := h.hashKey(key)
	data := hashValue[T]{
		key:   key,
		value: value,
	}

	for h.data[idx] != nil && h.data[idx].key != key {
		idx++
		idx = idx % h.capacity
	}
	h.data[idx] = &data
	h.size++

	// Resize data
	if (float64(h.size) / float64(h.capacity)) >= 0.5 {
		h.capacity = h.capacity * h.capacity
		newArr := make([]*hashValue[T], h.capacity)

		for _, v := range h.data {
			if v != nil {
				newIdx := h.hashKey(v.key)
				for newArr[newIdx] != nil && newArr[newIdx].key != key {
					newIdx++
					newIdx = newIdx % h.capacity
				}
				newArr[newIdx] = v
			}
		}
		h.data = newArr
	}
}

func (h *basicHashImpl[T]) Get(key string) (T, bool) {
	idx := h.hashKey(key)
	var zeroVal T
	if h.data[idx] == nil {
		return zeroVal, false
	}

	for h.data[idx] != nil && h.data[idx].key != key {
		idx++
		idx = idx % h.capacity
	}

	return h.data[idx].value, true
}

func (h *basicHashImpl[T]) hashKey(key string) int {
	var hashInt int
	for _, ch := range key {
		hashInt += int(ch)
	}

	idx := hashInt % h.capacity
	return idx
}
