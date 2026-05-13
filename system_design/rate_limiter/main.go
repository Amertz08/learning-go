package main

import (
	"context"
	"fmt"
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
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	bucketSize := 5

	limiter := NewTokenLimiter(bucketSize)
	limiter.Start(ctx)

	for i := 0; i < 1000; i++ {
		if limiter.Acquire() {
			fmt.Println("can acquire")
			time.Sleep(1 * time.Second)
		} else {
			fmt.Println("dropping")
			time.Sleep(3 * time.Second)
		}
	}
}
