package main

import (
	"context"
	"fmt"
	"math/rand"
	"time"
)

// SquareValues just squares the input value
func SquareValues(ctx context.Context, input int) int {
	time.Sleep(1 * time.Second)
	return input * input
}

// PrintValue simply prints the value from the channel with a name prefix and passes the value along
func PrintValue(name string) PipelineFunc {
	return func(ctx context.Context, input int) int {
		n := (rand.Intn(10) + 1) * 10
		time.Sleep(time.Duration(n) * time.Millisecond)
		fmt.Printf("%s: %d\n", name, input)
		return input
	}
}
