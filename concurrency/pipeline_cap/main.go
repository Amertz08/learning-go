package main

import (
	"context"
	"time"
)

/*
	The purpose of this program is to demonstrate a pipeline implementation and be a 'capstone' type example project

	Pipeline stages should take a channel as input and output a channel as output
	1. Generate values
	2. Implement a fan-out stage to distribute work across multiple goroutines
	3. Implement a fan-in stage to collect results from multiple goroutines
	4. Broadcast results to multiple consumers
*/

func main() {
}

func GenerateValues(ctx context.Context) <-chan int {
	output := make(chan int)

	go func() {
		defer close(output)
		for i := 0; ; i++ {
			select {
			case <-ctx.Done():
				return
			default:
				output <- i
				time.Sleep(100 * time.Millisecond)
			}
		}
	}()

	return output
}

func SquareValues(ctx context.Context, input <-chan int) <-chan int {
	output := make(chan int)

	go func() {
		defer close(output)
		for {
			select {
			case val, ok := <-input:
				if !ok {
					return
				}
				// Make this slower than the input to demonstrate pipeline bottlenecks
				time.Sleep(1 * time.Second)
				output <- val * val
			case <-ctx.Done():
				return
			}
		}
	}()

	return output
}
