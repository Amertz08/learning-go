package datastructures

type Matrix[T any] [][]T

func (m Matrix[T]) Rows() int {
	return len(m)
}

func (m Matrix[T]) Columns() int {
	return len(m[0])
}

func (m Matrix[T]) Transpose() [][]T {
	return nil
}
