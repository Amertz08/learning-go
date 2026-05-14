package main

import (
	"context"
	"fmt"
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

	// TODO: as soon as producer comes online consumer dies
	encDecoder := queue.NewJSONEncodeDecoder[internal.Message]()

	q, err := queue.NewRabbitMQImpl[internal.Message](
		"amqp://guest:guest@localhost:5672/",
		"hello",
		encDecoder,
	)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	defer q.Close()

	service.ConsumerService[internal.Message](ctx, q, workers)
}
