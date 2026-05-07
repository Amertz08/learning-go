package main

import (
	"context"
	"fmt"
	"math/rand"
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

	TODO
		- nil channel example
		- testing
*/

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	initialInput := GenerateValues(ctx, 100)

	// Pipeline starts here
	squaredResults := FanIn(ctx, 0, FanOut(ctx, 4, 0, initialInput, SquareValues))
	out := RunPipelineFunc(ctx, 100, PrintValue("printer1"), squaredResults)

	// TODO: pipeline after broadcast
	newConfs := []PipelineFuncConfig{
		{PrintValue("printer2"), 0},
		{PrintValue("printer3"), 0},
	}

	outs := Broadcast(ctx, out, newConfs...)

	// TODO: doesn't appear to be working
	finalOut := Consolidate(ctx, 0, outs[0], outs[1], PrintValue("Final printer"))

	// block until drained
	for _, o := range outs {
		<-o
	}
	<-finalOut
}

// PipelineFunc is a function that is the unit of work for any step in the pipeline
type PipelineFunc func(ctx context.Context, input int) int

// RunPipelineFunc handles running PipelineFunc and the lifecycle of their output channel
func RunPipelineFunc(
	ctx context.Context,
	buffSize int,
	pipeFunc PipelineFunc,
	inputChan <-chan int,
) <-chan int {
	output := make(chan int, buffSize)

	go func() {
		defer close(output)

		for {
			select {
			case <-ctx.Done():
				return
			case val, ok := <-inputChan:
				if !ok {
					return
				}
				output <- pipeFunc(ctx, val)
			}
		}
	}()

	return output
}

// FanOut will kick off multiple workers to process the input channel with the given pipeline function
func FanOut(
	ctx context.Context,
	workers, buffSize int,
	inChan <-chan int,
	pipeFunc PipelineFunc,
) []<-chan int {
	output := make([]<-chan int, workers)

	// A wait group is not needed here as the pipeFunc will close the output channel when done
	for i := 0; i < workers; i++ {
		output[i] = RunPipelineFunc(ctx, buffSize, pipeFunc, inChan)
	}
	return output

}

// PipelineFuncConfig is used to configure functions executed by Broadcast
type PipelineFuncConfig struct {
	Func       PipelineFunc
	BufferSize int
}

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

// SquareValues just squares the input value
func SquareValues(ctx context.Context, input int) int {
	time.Sleep(1 * time.Second)
	return input * input
}

// PrintValue simply prints the value from the channel with a name prefix and passes the value along
func PrintValue(name string) PipelineFunc {
	return func(ctx context.Context, input int) int {
		n := (rand.Intn(10) + 1) * 10
		time.Sleep(time.Duration(n) * time.Millisecond)
		fmt.Printf("%s: %d\n", name, input)
		return input
	}
}

// FanIn will combine multiple input channels into a single output channel
func FanIn(ctx context.Context, buffSize int, inChans []<-chan int) <-chan int {
	output := make(chan int, buffSize)

	// A wait group is needed here because we're creating the output channel in the scope of this function,
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

// Broadcast will multiplex a message from an input channel across many pipeline functions
func Broadcast(
	ctx context.Context,
	inCh <-chan int,
	pipeLineFuncConfigs ...PipelineFuncConfig,
) []<-chan int {
	newInputs := make([]chan int, len(pipeLineFuncConfigs))
	outputChans := make([]<-chan int, len(pipeLineFuncConfigs))
	// Make sure to close all input channels when done
	defer func() {
		for _, ch := range newInputs {
			if ch != nil {
				close(ch)
			}
		}
	}()

	go func() {
		for {
			select {
			case val, ok := <-inCh:
				if !ok {
					return
				}
				// We have a value from input so read the value and pass it to each pipeline function
				// This is where the broadcast happens
				for i, conf := range pipeLineFuncConfigs {
					if newInputs[i] == nil {
						// If we have not yet created a channel for this pipeline function, create one
						newInputs[i] = make(chan int, cap(inCh))
					}
					newInputs[i] <- val
					outputChans[i] = RunPipelineFunc(ctx, conf.BufferSize, conf.Func, newInputs[i])
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	return outputChans
}

// Consolidate will listen to 2 different channels and execute the same function across both
func Consolidate(
	ctx context.Context,
	buffSize int,
	leftChan <-chan int,
	rightChan <-chan int,
	pipeFunc PipelineFunc,
) <-chan int {
	output := make(chan int, buffSize)
	go func() {
		defer close(output)
		for {
			// This is to highlight how nil channels work. If an upstream channel is closed you will still read the
			// zero value for that channel and can cause an infinite loop given the channel is always ready.
			// To combat this set the channel to nil and it will effectively remove it from the case statement.
			// We then have to make sure we have some default case with an exit strategy in order to actually
			// leave the loop.
			select {
			case <-ctx.Done():
				return
			case val, ok := <-leftChan:
				if !ok {
					leftChan = nil
					break
				}
				output <- pipeFunc(ctx, val)
			case val, ok := <-rightChan:
				if !ok {
					rightChan = nil
					break
				}
				output <- pipeFunc(ctx, val)
			default:
				if rightChan == nil && leftChan == nil {
					return
				}
				// do something so if both channels close we're not in a busy loop
				time.Sleep(1 * time.Second)
			}
		}
	}()
	return output
}
