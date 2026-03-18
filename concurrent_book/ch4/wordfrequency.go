package main

import (
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strings"
	"sync"
	"time"
	"unicode"
)

func main() {
	mutex := sync.Mutex{}
	var frequency = make(map[string]int)

	startTime := time.Now()
	for i := 1000; i <= 1030; i++ {
		url := fmt.Sprintf("https://rfc-editor.org/rfc/rfc%d.txt", i)
		go countWords(url, frequency, &mutex)
	}
	duration := time.Since(startTime)
	time.Sleep(10 * time.Second)
	mutex.Lock()
	for word, count := range frequency {
		fmt.Printf("%s: %d\n", word, count)
	}
	mutex.Unlock()
	fmt.Println("Elapsed time:", duration)
}

func countWords(url string, frequency map[string]int, mutex *sync.Mutex) {
	resp, _ := http.Get(url)
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		panic(fmt.Sprintf("error fetching %s: %s", url, resp.Status))
	}

	body, _ := io.ReadAll(resp.Body)
	lines := strings.Split(string(body), "\n")
	for _, line := range lines {
		for _, word := range strings.Fields(line) {
			strippedWord := removePunctuation(word)
			matched, _ := regexp.Match(`^[a-zA-Z]+$`, []byte(strippedWord))
			if matched {
				mutex.Lock()
				frequency[strings.ToLower(strippedWord)]++
				mutex.Unlock()
			}
		}
	}
}

func removePunctuation(s string) string {
	var b strings.Builder
	for _, r := range s {
		if !unicode.IsPunct(r) {
			b.WriteRune(r)
		}
	}
	return b.String()
}
