package service

import (
	"context"
	"sync"
	"time"
)

// TODO: rework this to publish an actual value
func ProducerService[T any](ctx context.Context, workerCount int, queue QueueReader[T]) {
	var wg sync.WaitGroup

	var zeroVal T

	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
				default:
					queue.Publish(ctx, zeroVal)
					time.Sleep(100 * time.Millisecond)
				}
			}
		}()
	}
	wg.Wait()
}
