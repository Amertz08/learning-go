package main

import (
	"context"
	"sync"
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
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	out := PrintValues(ctx,
		FanIn(ctx,
			FanOut(ctx, 4, GenerateValues(ctx), SquareValues)))
	<-out
}

type PipeLineFunc func(ctx context.Context, in <-chan int) <-chan int

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

func PrintValues(ctx context.Context, input <-chan int) <-chan int {
	output := make(chan int)

	go func() {
		defer close(output)
		for val := range input {
			select {
			case <-ctx.Done():
				return
			default:
				println(val)
			}
		}
	}()

	return output
}

// FanOut will kick off multiple workers to process the input channel with the given pipeline function
func FanOut(ctx context.Context, workers int, in <-chan int, pipeFunc PipeLineFunc) []<-chan int {
	output := make([]<-chan int, workers)

	for i := 0; i < workers; i++ {
		output[i] = pipeFunc(ctx, in)
	}
	return output
}

// FanIn will combine multiple input channels into a single output channel
func FanIn(ctx context.Context, inChans []<-chan int) <-chan int {
	output := make(chan int)

	var wg sync.WaitGroup
	for _, ch := range inChans {
		wg.Add(1)
		go func(inCh <-chan int) {
			defer wg.Done()
			for val := range inCh {
				select {
				case output <- val:
					continue
				case <-ctx.Done():
					return
				}
			}
		}(ch)
	}

	go func() {
		wg.Wait()
		close(output)
	}()

	return output
}
