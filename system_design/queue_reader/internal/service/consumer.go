package service

import (
	"context"
	"sync"
)

func ConsumerService[T any](ctx context.Context, queue QueueReader[T], workerCount int) {
	var wg sync.WaitGroup
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			select {
			case <-ctx.Done():
				return
			case _, ok := <-queue.Read(ctx):
				if !ok {
					return
				}
			}
		}()
	}
	wg.Wait()
}
