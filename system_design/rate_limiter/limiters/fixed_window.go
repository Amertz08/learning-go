package limiters

import (
	"context"
	"time"

	"github.come/Amertz08/learning-go/datastructures"
)

type FixedWindowLimiter struct {
	ticker   *time.Ticker
	maxCount int
	queue    datastructures.Queue[int]
}

func (l *FixedWindowLimiter) Start(ctx context.Context) {
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-l.ticker.C:
				// reset count at end of window
				for i := l.queue.Len(); i > 0; i-- {
					l.queue.Dequeue()
				}
			}
		}
	}()
}

func (l *FixedWindowLimiter) Acquire() bool {
	if l.queue.Len() == l.maxCount {
		return false
	}
	l.queue.Enqueue(1)
	return true
}
