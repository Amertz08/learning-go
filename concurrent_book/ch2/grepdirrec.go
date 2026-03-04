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

	searchDir(searchTerm, dirName)
	time.Sleep(5 * time.Second)
}

func searchDir(searchTerm, dirName string) {
	fmt.Println("Searching directory:", dirName)
	entries, err := os.ReadDir(dirName)
	if err != nil {
		fmt.Println("Error reading directory:", err)
		return
	}
	for _, entry := range entries {
		if entry.IsDir() {
			go searchDir(searchTerm, entry.Name())
		} else {
			go searchFile(searchTerm, dirName+"/"+entry.Name())
		}
	}
}

func searchFile(searchTerm, fileName string) {
	fmt.Println("checking", fileName)
	content, err := os.ReadFile(fileName)
	if err != nil {
		fmt.Println("Error reading file:", err)
		return
	}
	if strings.Contains(string(content), searchTerm) {
		fmt.Println("FOUND", fileName)
	} else {
		fmt.Println("NOT FOUND", fileName)
	}
}
