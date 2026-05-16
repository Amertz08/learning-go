package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.come/Amertz08/learning-go/system_design/queue_reader/internal"
	"github.come/Amertz08/learning-go/system_design/queue_reader/internal/queue"
	"github.come/Amertz08/learning-go/system_design/queue_reader/internal/service"
)

func main() {
	workerCount := 10
	ctx := context.Background()

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

	msg := internal.Message{
		First: "adam",
		Last:  "mertz",
	}

	producer := service.NewProducer[internal.Message](
		slog.New(slog.NewJSONHandler(os.Stdout, nil)),
		workerCount,
		q,
	)
	producer.Start(ctx)

	for {
		producer.Publish(msg)
		time.Sleep(1 * time.Second)
	}
}
