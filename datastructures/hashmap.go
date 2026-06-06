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
	data     [defaultHashSize]hashValue[T]
	size     int
	capacity int
}

func NewHashMap[T any]() HashMap[T] {
	h := &basicHashImpl[T]{size: 0}
	h.capacity = defaultHashSize
	return h
}

func (h *basicHashImpl[T]) Put(key string, value T) error {
	//hashInt, err := strconv.Atoi(key)
	//if err != nil {
	//	return err
	//}
	//idx := hashInt % h.capacity
	//data := hashValue[T]{
	//	key:   key,
	//	value: value,
	//}
	//h.data[idx] = data
	return nil
}

func (h *basicHashImpl[T]) Get(key string) (T, bool) {
	var zeroVal T
	return zeroVal, false
}
