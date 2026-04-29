package main

// TakeUntil should consume the pipeline until 'f(val) == false' and then shuts down the pipeline
func TakeUntil[K any](f func(K) bool, quit chan int, input <-chan K) <-chan K {
	output := make(chan K)

	go func() {
		defer close(output)

		moreData := true
		shouldContinue := true
		var msg K

		for moreData && shouldContinue {
			select {
			case msg, moreData = <-input:
				if moreData {
					shouldContinue = f(msg)
					output <- msg
				}
			case <-quit:
				return
			}
		}
		if !shouldContinue {
			// We've reached the point where f(val) == false and thus we can shut down the pipeline
			close(quit)
		}
	}()
	return output
}
