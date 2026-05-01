package main

import (
	"fmt"
	"log"
	"os"
)

func main() {
	readSmallFile()
}

func readSmallFile() {
	data, err := os.ReadFile("file_processing/small.txt")
	if err != nil {
		log.Fatalf("error occurred reading: %s", err)
	}
	fmt.Println(string(data))
}
