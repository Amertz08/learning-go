package main

import (
	"context"
	"fmt"
	"time"

	"github.come/Amertz08/learning-go/system_design/rate_limiter/limiters"
)

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

	limiter := limiters.NewTokenLimiter(bucketSize)
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
