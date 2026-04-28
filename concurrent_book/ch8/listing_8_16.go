package main

import (
	"fmt"
	"math/rand"
	"time"
)

/*
	Write a main function using a select statement that continuously consumes from the output chanel, printing the output
	on the console until 5 seconds have elapsed from the start of the program, then exits.
*/

func generateNumbers() chan int {
	output := make(chan int)
	go func() {
		for {
			output <- rand.Intn(10)
			time.Sleep(200 * time.Millisecond)
		}
	}()

	return output
}

func main() {
	// initialize a timer channel
	start := time.After(5 * time.Second)
	outChannel := generateNumbers()
	for {
		select {
		// print numbers
		case num := <-outChannel:
			fmt.Println(num)
		// once we can read from the start channel we know 5 seconds have elapsed and can exit the program.
		case <-start:
			return
		}
	}
}
