package datastructures

import "errors"

var ErrOutOfBounds = errors.New("out of bounds")

// TODO: any is probably not correct. It should probably be anything that is +-
//
//	'constraints.Ordered' is close but has a string. So maybe make my own?
type Matrix[T any] interface {
	Set(int, int, T) error
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
	if min(r, c) < 0 {
		return zeroVal, ErrOutOfBounds
	}
	if r > len(m.data)-1 || c > len(m.data)-1 {
		return zeroVal, ErrOutOfBounds
	}
	return m.data[r][c], nil
}

func (m *matrixImpl[T]) Set(r, c int, val T) error {
	if min(r, c) < 0 {
		return ErrOutOfBounds
	}
	if r > len(m.data)-1 || c > len(m.data)-1 {
		return ErrOutOfBounds
	}

	m.data[r][c] = val

	return nil
}

func DotProduct[T any](a, b Matrix[T]) (Matrix[T], error) {
	return nil, nil
}
