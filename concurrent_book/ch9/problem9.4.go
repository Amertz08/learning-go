package main

// Drain reads from input channel and discards each message until quit signal is received
func Drain[T any](quit <-chan int, input <-chan T) {
	go func() {
		for {
			select {
			case <-input:
				continue

			case <-quit:
				return
			}
		}
	}()
}
