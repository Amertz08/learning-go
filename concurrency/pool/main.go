package main

import (
	"context"
	"fmt"
	"math/rand/v2"
	"sync"
	"time"
)

func main() {
	ctx := context.Background()
	// There is no real reason this channel cannot be buffered.
	// Creation of jobs takes significantly less time than consumption of them.
	inputChannel := make(chan *Job)
	go func(c context.Context) {
		defer close(inputChannel)
		for i := 0; i < 100; i++ {
			select {
			case <-c.Done():
				return
			case inputChannel <- &Job{Id: i}:
			}
		}
	}(ctx)

	// Create 10 workers
	maxWorkers := 10
	// Attempt 3 times to process a job before marking it as failed
	maxAttempts := 3
	var wg sync.WaitGroup

	for i := 0; i < maxWorkers; i++ {
		wg.Add(1)
		go func(c context.Context, in <-chan *Job) {
			defer wg.Done()
			for input := range in {
				for !input.Success && len(input.Errors) < maxAttempts {
					select {
					case <-c.Done():
						return
					default:
						input.DoWork()
					}
				}
			}
		}(ctx, inputChannel)
	}

	// Will block until all workers are done
	wg.Wait()

	// The requirements do state that "No job should be lost."
	// I did not clarify what that meant w/ ChatGPT (probably could ask it.)
	// My interpretation is that jobs should not be dropped or lost during processing.
	// If the process is canceled, however, jobs should be processed up to the point of cancellation.
	// The way this is currently written, the WIP job could still have attempts left.
	// An alternative approach would be to use a buffered channel for input jobs and drain it before exiting.
}

type Job struct {
	Id      int
	Success bool
	Errors  []error
}

func (j *Job) String() string {
	return fmt.Sprintf("Job %d: %v", j.Id, j.Success)
}

func (j *Job) DoWork() {
	minTime := 50 * time.Millisecond
	maxTime := 2 * time.Second
	randomDuration := minTime + time.Duration(rand.Int64N(int64(maxTime-minTime)))
	time.Sleep(randomDuration)

	// Fail 30% of the time
	j.Success = rand.IntN(10) >= 3
	if !j.Success {
		j.Errors = append(j.Errors, fmt.Errorf("failed job %d", j.Id))
	}
	fmt.Println(j)
}
