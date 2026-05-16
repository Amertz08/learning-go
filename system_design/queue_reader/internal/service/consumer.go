package service

import (
	"context"
	"fmt"
	"sync"
)

func ConsumerService[T any](ctx context.Context, queue QueueReader[T], workerCount int) {
	var wg sync.WaitGroup
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					fmt.Println("context cancelled")
					return
				case msg, ok := <-queue.Read(ctx):
					if !ok {
						fmt.Println("exiting")
						return
					}
					fmt.Println(msg)
				}
			}
		}()
	}
	wg.Wait()
}
