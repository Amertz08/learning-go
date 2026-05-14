package main

import (
	"context"
	"time"

	"github.come/Amertz08/learning-go/system_design/queue_reader/internal/service"
)

func main() {
	workers := 10
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// TODO init actual queue implementation

	var queue service.QueueReader[int]
	service.ProducerService[int](ctx, workers, queue)

}
