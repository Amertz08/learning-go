package main

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"
)

type tokens chan struct{}

type TokenLimiter struct {
	count  int
	tokens tokens
	ticker *time.Ticker
}

func NewTokenLimiter(count int) *TokenLimiter {
	// TODO: configurable timer
	toks := make(chan struct{}, count)
	for i := 0; i < count; i++ {
		toks <- struct{}{}
	}

	return &TokenLimiter{
		count:  count,
		tokens: toks,
		ticker: time.NewTicker(2 * time.Second),
	}
}

func (l *TokenLimiter) Start(ctx context.Context) {
	go func() {
		for {
			select {
			case <-l.ticker.C:
				select {
				case l.tokens <- struct{}{}:
				default:
				}
			case <-ctx.Done():
				return
			}
		}
	}()
}

func (l *TokenLimiter) Acquire() bool {
	select {
	case <-l.tokens:
		return true
	default:
		return false
	}
}

func main() {
	// script showing bucket rate limiting but how do I integrate this into an API?
	// TODO: use timer
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
