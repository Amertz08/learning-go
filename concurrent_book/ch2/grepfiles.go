package main

import (
	"fmt"
	"os"
	"strings"
	"time"
)

func main() {
	searchTerm := os.Args[1]
	fileNames := os.Args[2:]

	for _, fileName := range fileNames {
		go func(term, fn string) {
			content, err := os.ReadFile(fn)
			if err != nil {
				fmt.Println(err)
				return
			}
			contentStr := string(content)
			if strings.Contains(contentStr, term) {
				fmt.Println(fn, contentStr)
			}
		}(searchTerm, fileName)
	}
	time.Sleep(3 * time.Second)
}
