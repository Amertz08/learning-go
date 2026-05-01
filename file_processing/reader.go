package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"os"
)

const buffSize = 4096

func main() {
	chunkStreamLargeFile()
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

func chunkStreamLargeFile() {
	file, err := os.Open("file_processing/large_file.bin")
	if err != nil {
		log.Fatalf("error reading large file: %s", err)
	}
	defer file.Close()

	buf := make([]byte, buffSize)

	for {
		n, err := file.Read(buf)
		if n > 0 {
			fmt.Println(string(buf[:n]))
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatalf("error reading into buffer: %s", err)
		}
	}
}
