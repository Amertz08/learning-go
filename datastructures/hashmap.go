package datastructures

const defaultHashSize = 2

type HashMap[T any] interface {
	Put(key string, value T) error
	Get(key string) (T, bool)
}

type hashValue[T any] struct {
	key   string
	value T
}

type basicHashImpl[T any] struct {
	data     [defaultHashSize]*hashValue[T]
	size     int
	capacity int
}

func NewHashMap[T any]() HashMap[T] {
	h := &basicHashImpl[T]{size: 0}
	h.capacity = defaultHashSize
	return h
}

func (h *basicHashImpl[T]) Put(key string, value T) error {
	idx := h.hashKey(key)
	data := hashValue[T]{
		key:   key,
		value: value,
	}
	// TODO: pretty sure this will infinite loop if
	//		the array was full
	for h.data[idx] != nil {
		idx++
	}
	h.data[idx] = &data
	return nil
}

func (h *basicHashImpl[T]) Get(key string) (T, bool) {
	idx := h.hashKey(key)
	var zeroVal T
	if h.data[idx] == nil {
		return zeroVal, false
	}

	// TODO: pretty sure this will infinite loop if
	//		the array was full
	for h.data[idx] != nil && h.data[idx].key != key {
		idx++
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
