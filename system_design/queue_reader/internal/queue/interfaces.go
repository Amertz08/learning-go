package queue

// EncodeDecoder should convert a type to and from []byte
type EncodeDecoder[T any] interface {
	Encode(val T) ([]byte, error)
	Decode(val []byte) (T, error)
}
