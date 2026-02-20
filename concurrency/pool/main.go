package main

import (
	"context"
	"sync"
)

func main() {
	ctx := context.Background()
	inputChannel := make(chan int)
	go func(c context.Context) {
		defer close(inputChannel)
		for i := 0; i < 100; i++ {
			select {
			case <-c.Done():
				return
			case inputChannel <- i:
			}
		}
	}(ctx)

	maxWorkers := 10
	var wg sync.WaitGroup

	for i := 0; i < maxWorkers; i++ {
		wg.Add(1)
		go func(c context.Context, in <-chan int) {
			defer wg.Done()
			for input := range in {
				select {
				case <-c.Done():
					return
				default:
					println(input)
				}
			}
		}(ctx, inputChannel)
	}

	wg.Wait()
}
