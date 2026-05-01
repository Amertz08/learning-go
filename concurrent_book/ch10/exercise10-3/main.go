package main

import (
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"
)

func main() {
	const pagesToDownload = 30
	start := time.Now()
	loopConcurrent(pagesToDownload)
	fmt.Printf("time: %d\n", time.Since(start)/time.Millisecond)

}

func syncDownload(pageCount int) {
	totalLines := 0
	for i := 1000; i < 1000+pageCount; i++ {
		url := fmt.Sprintf("https://rfc-editor.org/rfc/rfc%d.txt", i)
		fmt.Println("Downloading", url)
		resp, _ := http.Get(url)
		if resp.StatusCode != 200 {
			panic("Server's error: " + resp.Status)
		}
		bodyBytes, _ := io.ReadAll(resp.Body)
		totalLines += strings.Count(string(bodyBytes), "\n")
		resp.Body.Close()
	}
	fmt.Println("Total lines:", totalLines)
}

func loopConcurrent(pageCount int) {
	totalLines := 0
	mut := sync.Mutex{}
	var wg sync.WaitGroup
	for i := 1000; i < 1000+pageCount; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			url := fmt.Sprintf("https://rfc-editor.org/rfc/rfc%d.txt", i)
			fmt.Println("Downloading", url)
			resp, _ := http.Get(url)
			if resp.StatusCode != 200 {
				fmt.Println("Server's error: " + resp.Status)
				return
			}
			bodyBytes, _ := io.ReadAll(resp.Body)
			mut.Lock()
			totalLines += strings.Count(string(bodyBytes), "\n")
			mut.Unlock()
			resp.Body.Close()
		}(i)
	}
	wg.Wait()
	fmt.Println("Total lines:", totalLines)
}
