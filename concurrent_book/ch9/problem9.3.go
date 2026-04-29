package main

import "fmt"

// Print reads from input channel and prints each message until quit signal is received
func Print[T any](quit <-chan int, input <-chan T) <-chan T {
	output := make(chan T)
	go func() {
		defer close(output)

		var msg T
		moreData := true
		for moreData {
			select {
			case msg, moreData = <-input:
				if moreData {
					fmt.Println(msg)
					output <- msg
				}
			case <-quit:
				return
			}
		}
	}()
	return output
}
