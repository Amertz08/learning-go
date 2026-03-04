package main

import (
	"fmt"
	"os"
	"strings"
	"time"
)

func main() {
	searchTerm := os.Args[1]
	dirName := os.Args[2]

	entries, err := os.ReadDir(dirName)
	if err != nil {
		fmt.Println(err)
		return
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		go func(searchTerm, fileName string) {
			fmt.Println(fileName)
			content, err := os.ReadFile(fileName)
			if err != nil {
				fmt.Println(err)
				return
			}
			if strings.Contains(string(content), searchTerm) {
				fmt.Println(fileName, string(content))
			}
		}(searchTerm, entry.Name())

	}
	time.Sleep(3 * time.Second)
}
