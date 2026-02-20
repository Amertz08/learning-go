package main

import (
	"context"
	"fmt"
	"sync"
)

func main() {
	ctx := context.Background()
	inputChannel := make(chan Job)
	go func(c context.Context) {
		defer close(inputChannel)
		for i := 0; i < 100; i++ {
			select {
			case <-c.Done():
				return
			case inputChannel <- Job{Id: i}:
			}
		}
	}(ctx)

	maxWorkers := 10
	var wg sync.WaitGroup

	for i := 0; i < maxWorkers; i++ {
		wg.Add(1)
		go func(c context.Context, in <-chan Job) {
			defer wg.Done()
			for input := range in {
				select {
				case <-c.Done():
					return
				default:
					fmt.Println(input)
				}
			}
		}(ctx, inputChannel)
	}

	wg.Wait()
}

type Job struct {
	Id      int
	Success bool
	Errors  []error
}

func (j Job) String() string {
	return fmt.Sprintf("Job %d: %v", j.Id, j.Success)
}
