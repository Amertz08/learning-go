package datastructures

type HashMap[T any] interface {
	Put(key string, value T)
	Get(key string) T
}
