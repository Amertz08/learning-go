package main

import (
	"fmt"
	"os"
	"time"
)

func main() {
	fileNames := os.Args[1:]
	for _, fileName := range fileNames {
		go func(fn string) {
			content, err := os.ReadFile(fn)
			if err != nil {
				fmt.Println(err)
				return
			}

			fmt.Println(fn, string(content))

		}(fileName)
	}
	time.Sleep(3 * time.Second)
}
