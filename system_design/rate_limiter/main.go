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
	toks := make(tokens, count)
	for i := 0; i < count; i++ {
		toks <- struct{}{}
	}

	return &TokenLimiter{
		count:  count,
		tokens: toks,
		ticker: time.NewTicker(2 * time.Second),
	}
}

// Start will kick off a goroutine to add tokens to the bucket
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

// Acquire returns true if a token can be retrieved from the bucket false otherwise
func (l *TokenLimiter) Acquire() bool {
	select {
	case <-l.tokens:
		return true
	default:
		return false
	}
}

func main() {
	/*
		token limiter

		could use as follows very WIP idea
		func Middleware(limiter TokenLimiter) func(http.Handler) http.Handler {
			// somehow get ctx here
			limiter.Start(ctx)

			return func(next http.Handler) http.Handler {
				return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if limiter.Aquire() {
						next.ServeHTTP(w, r)
					} else {
						// write 429
					}
				})
			}
		}

	*/
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
