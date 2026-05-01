package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
)

func main() {
	streamLargeFile()
}

func readSmallFile() {
	data, err := os.ReadFile("file_processing/small.txt")
	if err != nil {
		log.Fatalf("error occurred reading: %s", err)
	}
	fmt.Println(string(data))
}

func streamLargeFile() {
	file, err := os.Open("file_processing/large_file.bin")
	if err != nil {
		log.Fatalf("error reading large file: %s", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		line := scanner.Text()
		fmt.Println(line)
	}
	if err := scanner.Err(); err != nil {
		log.Fatalf("error scanning file: %s", err)
	}
}
