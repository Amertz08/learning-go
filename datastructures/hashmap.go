package datastructures

type HashMap[T any] interface {
	Put(key string, value T) error
	Get(key string) T
}

type hashValue[T any] struct {
	key   string
	value T
}

type basicHashImpl[T any] struct {
	data     []hashValue[T]
	size     int
	capacity int
}

func CreateHashMap[T any]() HashMap[T] {
	default_cap := 2
	return &basicHashImpl[T]{data: make([]hashValue[T], default_cap), size: 0, capacity: default_cap}
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

func (h *basicHashImpl[T]) Get(key string) T {
	var zeroVal T
	return zeroVal
}
