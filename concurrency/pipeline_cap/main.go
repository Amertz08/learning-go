package main

import (
	"context"
	"fmt"
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

	out := PrintValues("printer1")(ctx, 100,
		FanIn(ctx,
			FanOut(ctx, 4, 0, GenerateValues(ctx, 100), SquareValues)))
	<-out
}

// PipeLineFunc represents a function that takes a context and an input channel and returns an output channel.
type PipeLineFunc func(ctx context.Context, buffSize int, in <-chan int) <-chan int

// GenerateValues creates a channel that emits an infinite sequence of integers, respecting context cancellation signals.
func GenerateValues(ctx context.Context, buffSize int) <-chan int {
	output := make(chan int, buffSize)

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

// SquareValues reads integers from the input channel, squares them, and sends the results to the output channel.
func SquareValues(ctx context.Context, buffSize int, input <-chan int) <-chan int {
	output := make(chan int, buffSize)

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

// PrintValues takes a name and returns a PipeLineFunc that logs channel values with the given name prefix.
func PrintValues(name string) PipeLineFunc {
	return func(ctx context.Context, buffSize int, input <-chan int) <-chan int {
		output := make(chan int, buffSize)

		go func() {
			defer close(output)
			for val := range input {
				select {
				case <-ctx.Done():
					return
				default:
					fmt.Printf("%s: %d\n", name, val)
				}
			}
		}()

		return output
	}
}

// FanOut will kick off multiple workers to process the input channel with the given pipeline function
func FanOut(ctx context.Context, workers int, buffSize int, in <-chan int, pipeFunc PipeLineFunc) []<-chan int {
	output := make([]<-chan int, workers)

	// A wait group is not needed here as the pipeFunc will close the output channel when done
	for i := 0; i < workers; i++ {
		output[i] = pipeFunc(ctx, buffSize, in)
	}
	return output
}

// FanIn will combine multiple input channels into a single output channel
func FanIn(ctx context.Context, inChans []<-chan int) <-chan int {
	output := make(chan int)

	// A wait group is needed here because we're creating the output channel in the scope of this function
	// and it needs to close the output channel when it's goroutines are done.
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

// Broadcast will duplex a message from an input channel across many pipeline functions
func Broadcast(ctx context.Context, buffSize int, inCh <-chan int, pipeLineFuncs ...PipeLineFunc) {
	newInputs := make([]chan int, len(pipeLineFuncs))
	// Make sure to close all input channels when done
	defer func() {
		for _, ch := range newInputs {
			if ch != nil {
				close(ch)
			}
		}
	}()

	for {
		select {
		case val, ok := <-inCh:
			if !ok {
				return
			}
			// We have a value from input so read the value and pass it to each pipeline function
			for i, pipeFunc := range pipeLineFuncs {
				if newInputs[i] == nil {
					// If we have not yet created a channel for this pipeline function, create one
					newInputs[i] = make(chan int)
				}
				newInputs[i] <- val
				// TODO: I think we should be gathering the channels then returning them somehow
				pipeFunc(ctx, buffSize, newInputs[i])
			}
		case <-ctx.Done():
			return
		}
	}
	// TODO: should there be a return?
}
