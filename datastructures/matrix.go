package datastructures

type Matrix[T any] interface {
	Get(int, int) (T, error)
	Rows() int
	Columns() int
	Transpose() Matrix[T]
}
type matrixImpl[T any] struct {
	data [][]T
}

func NewMatrix[T any](rows, columns int) Matrix[T] {
	d := make([][]T, rows)
	for i := 0; i < rows; i++ {
		d[i] = make([]T, columns)
	}
	return &matrixImpl[T]{data: d}
}

func (m *matrixImpl[T]) Rows() int {
	return len(m.data)
}

func (m *matrixImpl[T]) Columns() int {
	return len(m.data[0])
}

func (m *matrixImpl[T]) Transpose() Matrix[T] {
	return nil
}

func (m *matrixImpl[T]) Get(r, c int) (T, error) {
	var zeroVal T
	return zeroVal, nil
}
