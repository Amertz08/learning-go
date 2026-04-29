package main

import (
	"time"
)

func GenerateSquares(quit <-chan int) <-chan int {
	out := make(chan int)
	go func() {
		defer close(out)
		for i := 1; ; i++ {
			select {
			case <-quit:
				break
			default:
				out <- i * i
				time.Sleep(time.Millisecond * 250)
			}
		}
	}()
	return out
}
