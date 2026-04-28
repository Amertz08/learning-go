package main

import (
	"fmt"
	"math/rand"
	"time"
)

/*
Write a main function, using a `select` statement, that reads messages coming from generateTemp() and sends
only the latest temperature to the outputTemp channel?
*/

func main() {
	outCh := generateTemp()
	inCh := make(chan int)
	outputTemp(inCh)

	// Get an initial temp which will open the channel for a new value to be generated
	// This will also allow the select statement to execute the case where inCh is available which is the fist iteration.
	t := <-outCh

	for {
		select {
		// keep reassigning a temp
		case t = <-outCh:
		// executes this case once inCh is available
		case inCh <- t:
		}
	}
}

func generateTemp() chan int {
	output := make(chan int)

	go func() {
		temp := 50
		for {
			// Will only publish if there is a reader
			output <- temp
			temp += rand.Intn(3) - 1
			time.Sleep(200 * time.Millisecond)
		}
	}()

	return output
}

func outputTemp(input chan int) {
	go func() {
		for {
			// blocks until something is available
			curTemp := <-input
			// print value
			fmt.Println("Current temp:", curTemp)
			// go to sleep
			time.Sleep(2 * time.Second)
		}
	}()
}
