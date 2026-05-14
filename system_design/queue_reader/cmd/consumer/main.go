package main

import (
	"context"

	"github.come/Amertz08/learning-go/system_design/queue_reader/internal/queue"
	"github.come/Amertz08/learning-go/system_design/queue_reader/internal/service"
)

func main() {
	workers := 5
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// TODO: init actual queue implementation
	q := queue.NewRabbitMQImpl[int](
		"",
		"hello",
	)

	service.ConsumerService[int](ctx, q, workers)
}
