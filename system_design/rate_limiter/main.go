package main

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"
)

func main() {
	// script showing bucket throttling but how do I integrate this into an API?
	ctx := context.Background()
	bucketSize := 5
	bucket := make(chan int, bucketSize)
	defer close(bucket)

	// initially fill channel
	for i := 0; i < bucketSize; i++ {
		bucket <- 1
	}

	// filler process
	go func(ctx context.Context, buk chan<- int) {
		for {
			select {
			case <-ctx.Done():
				return
			case buk <- 1:
				time.Sleep(5 * time.Second)
			default:
				time.Sleep(5 * time.Second)
			}
		}
	}(ctx, bucket)

	inputChan := make(chan int)
	defer close(inputChan)
	// input generator
	go func(ctx context.Context, input chan<- int) {
		for {
			select {
			case <-ctx.Done():
				return
			case input <- 1:
			default:
				time.Sleep(1 * time.Second)
			}
		}
	}(ctx, inputChan)

	var wg sync.WaitGroup
	wg.Add(1)
	go func(ctx context.Context, buk <-chan int, input <-chan int) {
		defer wg.Done()
		// have value come in
		// can we read off the channel?
		// if so process value
		// else reject
		for {
			select {
			case <-ctx.Done():
				return
			case <-input:
				select {
				case <-ctx.Done():
					return
				case <-buk:
					// process
					fmt.Println("process")
				default:
					// reject
					fmt.Println("reject")
					// sleep between 1 and 2 seconds if rejected
					time.Sleep(time.Duration(rand.Intn(3)+1) * time.Second)
				}
			}
		}
	}(ctx, bucket, inputChan)

	wg.Wait()
}
