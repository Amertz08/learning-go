package main

import (
	"context"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	workers := 5
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var queue QueueReader[int]
	defer queue.Close()

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			select {
			case <-ctx.Done():
				return
			case _, ok := <-queue.Read(ctx):
				if !ok {
					return
				}
			}
		}()
	}
	wg.Wait()
}

type QueueReader[T any] interface {
	Close() error
	Publish(ctx context.Context, val T) error
	Read(ctx context.Context) <-chan T
}

// EncodeDecoder should convert a type to and from []byte
type EncodeDecoder[T any] interface {
	Encode(T any) ([]byte, error)
	Decode(val []byte) (T, error)
}

type RabbitMQImpl[T any] struct {
	conn          *amqp.Connection
	channel       *amqp.Channel
	name          string
	encodeDecoder EncodeDecoder[T]
}

func NewRabbitMQImpl[T any](
	connStr, queueName string,
	encodeDecoder EncodeDecoder[T],
) (*RabbitMQImpl[T], error) {
	conn, err := amqp.Dial(connStr)
	if err != nil {
		return nil, err
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, err
	}
	q := &RabbitMQImpl[T]{
		conn:          conn,
		channel:       ch,
		name:          queueName,
		encodeDecoder: encodeDecoder,
	}
	return q, nil
}

func (q *RabbitMQImpl[T]) Close() error {
	if err := q.conn.Close(); err != nil {
		return err
	}
	if err := q.channel.Close(); err != nil {
		return err
	}
	return nil
}

func (q *RabbitMQImpl[T]) Read(ctx context.Context) <-chan T {
	msgs, err := q.channel.ConsumeWithContext(ctx,
		q.name, // queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,
	)

	if err != nil {
		return nil
	}

	output := make(chan T)

	go func() {
		defer close(output)
		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-msgs:
				if !ok {
					return
				}
				val, decodeErr := q.encodeDecoder.Decode(msg.Body)
				if decodeErr != nil {
					// TODO: do something
					continue
				}
				output <- val
			}
		}
	}()
	return output
}

func (q *RabbitMQImpl[T]) Publish(ctx context.Context, val T) error {
	enc, err := q.encodeDecoder.Encode(val)
	if err != nil {
		return err
	}
	// TODO: is this the content type we want?
	err = q.channel.PublishWithContext(ctx,
		"",     // exchange
		q.name, // routing key
		false,  // mandatory
		false,  // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        enc,
		})
	return err
}
