package queue

import (
	"bytes"
	"encoding/json"
)

type jsonEncodeDecoder[T any] struct {
}

func NewJSONEncodeDecoder[T any]() *jsonEncodeDecoder[T] {
	return &jsonEncodeDecoder[T]{}
}

func (e *jsonEncodeDecoder[T]) Encode(val T) ([]byte, error) {
	var buff bytes.Buffer
	if err := json.NewEncoder(&buff).Encode(&val); err != nil {
		return nil, err
	}
	return buff.Bytes(), nil
}

func (e *jsonEncodeDecoder[T]) Decode(val []byte) (T, error) {
	var d T
	if err := json.NewDecoder(bytes.NewReader(val)).Decode(&d); err != nil {
		return d, err
	}
	return d, nil
}
