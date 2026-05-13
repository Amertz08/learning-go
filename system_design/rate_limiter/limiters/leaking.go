package limiters

import (
	"context"
	"time"

	"github.come/Amertz08/learning-go/datastructures"
)

// TODO no great way to integrate into middleware

type LeakingBucketLimiter[T any] struct {
	queue  datastructures.Queue[T]
	size   int
	ticker *time.Ticker
}

func NewLeakingBucketLimiter[T any](size int, procInterval time.Duration) *LeakingBucketLimiter[T] {
	return &LeakingBucketLimiter[T]{
		queue:  datastructures.NewLinkedListQueue[T](),
		size:   size,
		ticker: time.NewTicker(procInterval),
	}
}

// TODO: processFunc signature isn't very universal/flexible
func (l *LeakingBucketLimiter[T]) Start(ctx context.Context, processFunc func(...any)) {
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-l.ticker.C:
				val, ok := l.queue.Dequeue()
				if !ok {
					continue
				}
				processFunc(val)
			}
		}
	}()
}

func (l *LeakingBucketLimiter[T]) Add(input T) bool {
	if l.queue.Len() == l.size {
		return false
	}
	l.queue.Enqueue(input)
	return true
}
