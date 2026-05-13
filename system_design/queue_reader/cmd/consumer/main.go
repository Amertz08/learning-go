package main

import (
	"context"
	"fmt"
	"os"

	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		fmt.Println("failed to connect to queue")
		os.Exit(1)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		fmt.Println("failed to open channel")
		os.Exit(1)
	}
	defer ch.Close()

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
		fmt.Println("failed to create queue")
		os.Exit(1)
	}

	msgs, err := ch.ConsumeWithContext(
		ctx,
		q.Name, // queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	for {
		select {
		case <-ctx.Done():
		case msg, ok := <-msgs:
			if !ok {
				return
			}
			fmt.Println(msg)
		}
	}
}
