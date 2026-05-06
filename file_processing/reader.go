package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"os"
	"sync"
	"time"
)

const buffSize = 4096

func main() {
	// TODO: CLI flags to pick a method
	start := time.Now()
	chunkConcurrentStreamLargeFile()
	fmt.Println("took:", time.Since(start))
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

// At least in the contrived example this one is actually slower than just streaming directly
func chunkConcurrentStreamLargeFile() {
	file, err := os.Open("file_processing/large_file.bin")
	if err != nil {
		log.Fatalf("error reading large file: %s", err)
	}
	defer file.Close()

	stat, err := file.Stat()
	if err != nil {
		log.Fatalf("error getting file stats: %s", err)
	}

	fileSize := stat.Size()
	// TODO: I think we also need to do mod := fileSize % buffSize then if mod != 0 -> chunkCount++
	//		Because if there is a remainder there is 1 more chunk left
	//		I generated a 1GB file and thus no remainder
	chunkCount := fileSize / buffSize
	if fileSize%buffSize > 0 {
		chunkCount++
	}

	var wg sync.WaitGroup
	for i := int64(0); i < chunkCount; i++ {
		wg.Add(1)
		go func(offset int64) {
			defer wg.Done()
			buf := make([]byte, buffSize)

			_, err := file.ReadAt(buf, offset)
			if err != nil {
				log.Println("error read at: %s", err)
				return
			}
			fmt.Println(string(buf))
		}(buffSize * i)
	}
	wg.Wait()
}
