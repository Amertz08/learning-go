package main

import (
	"fmt"
	"log"
	"os"
)

func main() {
	data, err := os.ReadFile("small.txt")
	if err != nil {
		log.Fatalf("error occurred reading: %s", err)
	}
	fmt.Println(data)
}
