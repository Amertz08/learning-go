package service

import "context"

type QueueReader[T any] interface {
	Close() error
	Publish(ctx context.Context, val T) error
	Read(ctx context.Context) <-chan T
}
