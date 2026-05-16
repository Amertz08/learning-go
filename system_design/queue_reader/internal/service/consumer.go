package service

import (
	"context"
	"log/slog"
	"sync"
)

func ConsumerService[T any](
	ctx context.Context,
	logger *slog.Logger,
	queue QueueReader[T],
	workerCount int,
) {
	var wg sync.WaitGroup
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case msg, ok := <-queue.Read(ctx):
					if !ok {
						logger.Info("exiting")
						return
					}
					logger.Info("message received", slog.Any("msg", msg))
				}
			}
		}()
	}
	wg.Wait()
}
