package main

import (
	"context"
	"os"
	"time"

	"github.come/Amertz08/learning-go/system_design/queue_reader/internal"
	"github.come/Amertz08/learning-go/system_design/queue_reader/internal/queue"
	"github.come/Amertz08/learning-go/system_design/queue_reader/internal/service"
)

func main() {
	workers := 10
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	encDecoder := queue.NewJSONEncoderDecoder[internal.Message]()

	q, err := queue.NewRabbitMQImpl[internal.Message](
		"",
		"hello",
		encDecoder,
	)
	if err != nil {
		os.Exit(1)
	}
	defer q.Close()
	service.ProducerService[internal.Message](ctx, workers, q)

}
