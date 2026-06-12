package service

import (
	"context"
	"log/slog"
	"sync"
)

type Producer[T any] struct {
	logger      *slog.Logger
	messages    chan T
	queue       QueueReadWriter[T]
	workerCount int
}

func NewProducer[T any](logger *slog.Logger, workerCount int, queue QueueReadWriter[T]) *Producer[T] {
	return &Producer[T]{
		logger:      logger,
		messages:    make(chan T),
		queue:       queue,
		workerCount: workerCount,
	}
}

func (p *Producer[T]) Start(ctx context.Context) {
	var wg sync.WaitGroup

	for i := 0; i < p.workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
				case msg, ok := <-p.messages:
					if !ok {
						return
					}
					err := p.queue.Publish(ctx, msg)
					if err != nil {
						p.logger.Error("error publishing", slog.Any("error", err))
						return
					}
				}
			}
		}()
	}
}

func (p *Producer[T]) Publish(val T) {
	p.messages <- val
}
