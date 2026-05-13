package limiters

import (
	"context"
	"time"
)

type FixedWindowLimiter[T any] struct {
	ticker    *time.Ticker
	maxCount  int
	inputChan chan T
}

func (l *FixedWindowLimiter[T]) Start(ctx context.Context, procRequest func(T)) {
	go func() {
		reqCount := 0
		for {
			select {
			// TODO: do I need a default case? If nothing is in the channel I think
			// 	we will read the zero value I believe and process that. Which we probably don't want.
			case <-ctx.Done():
				return
			case <-l.ticker.C:
				reqCount = 0
			case val, ok := <-l.inputChan:
				if !ok {
					return
				}
				// if reqCount == maxCount ignore
				// else process request and increment count
				if reqCount == l.maxCount {
					continue
				} else {
					procRequest(val)
					reqCount++
				}
			}
		}
	}()
}

func (l *FixedWindowLimiter[T]) Add(val T) {
	l.inputChan <- val
}
