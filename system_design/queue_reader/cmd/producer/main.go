package main

import (
	"context"
	"os"
	"sync"
	"time"

	"github.come/Amertz08/learning-go/system_design/queue_reader/internal"
	"github.come/Amertz08/learning-go/system_design/queue_reader/internal/queue"
)

func main() {
	workerCount := 10
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	encDecoder := queue.NewJSONEncodeDecoder[internal.Message]()

	q, err := queue.NewRabbitMQImpl[internal.Message](
		"amqp://guest:guest@localhost:5672/",
		"hello",
		encDecoder,
	)
	if err != nil {
		os.Exit(1)
	}
	defer q.Close()
	var wg sync.WaitGroup

	msg := internal.Message{
		First: "adam",
		Last:  "mertz",
	}

	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
				default:
					q.Publish(ctx, msg)
					time.Sleep(100 * time.Millisecond)
				}
			}
		}()
	}
	wg.Wait()

}
