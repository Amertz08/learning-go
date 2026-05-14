package main

import (
	"context"
	"os"

	"github.come/Amertz08/learning-go/system_design/queue_reader/internal"
	"github.come/Amertz08/learning-go/system_design/queue_reader/internal/queue"
	"github.come/Amertz08/learning-go/system_design/queue_reader/internal/service"
)

func main() {
	workers := 5
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	encDecoder := queue.NewJSONEncodeDecoder[internal.Message]()

	q, err := queue.NewRabbitMQImpl[internal.Message](
		"",
		"hello",
		encDecoder,
	)
	if err != nil {
		os.Exit(1)
	}
	defer q.Close()

	service.ConsumerService[internal.Message](ctx, q, workers)
}
