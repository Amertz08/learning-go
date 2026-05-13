package main

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	workers := 10
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		fmt.Println("error dialing rabbitmq")
		os.Exit(1)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		fmt.Println("error creating channel")
		os.Exit(1)
	}

	q, err := ch.QueueDeclare(
		"hello", // name
		true,    // durability
		false,   // delete when unused
		false,   // exclusive
		false,   // no-wait
		amqp.Table{
			amqp.QueueTypeArg: amqp.QueueTypeQuorum,
		},
	)
	if err != nil {
		fmt.Println("error creating queue")
		os.Exit(1)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	body := "hello"
	var wg sync.WaitGroup

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
				default:
					err = ch.PublishWithContext(ctx,
						"",     // exchange
						q.Name, // routing key
						false,  // mandatory
						false,  // immediate
						amqp.Publishing{
							ContentType: "text/plain",
							Body:        []byte(body),
						})
					time.Sleep(100 * time.Millisecond)
				}
			}
		}()
	}
	wg.Wait()

}
