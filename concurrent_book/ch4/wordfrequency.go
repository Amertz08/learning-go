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

type WordFrequency struct {
	frequency map[string]int
	mutex     *sync.Mutex
}

func NewWordFrequency() *WordFrequency {
	return &WordFrequency{
		frequency: make(map[string]int),
		mutex:     &sync.Mutex{},
	}
}

func (wf *WordFrequency) Count(word string) {
	wf.mutex.Lock()
	wf.frequency[strings.ToLower(word)]++
	wf.mutex.Unlock()
}

func (wf *WordFrequency) Counts() map[string]int {
	return wf.frequency
}

func (wf *WordFrequency) Lock() {
	wf.mutex.Lock()
}

func (wf *WordFrequency) Unlock() {
	wf.mutex.Unlock()
}

func main() {
	var frequency = NewWordFrequency()

	startTime := time.Now()
	for i := 1000; i <= 1030; i++ {
		url := fmt.Sprintf("https://rfc-editor.org/rfc/rfc%d.txt", i)
		go countWords(url, frequency)
	}
	duration := time.Since(startTime)
	time.Sleep(10 * time.Second)
	frequency.Lock()
	for word, count := range frequency.Counts() {
		fmt.Printf("%s: %d\n", word, count)
	}
	frequency.Unlock()
	fmt.Println("Elapsed time:", duration)
}

func countWords(url string, frequency *WordFrequency) {
	resp, _ := http.Get(url)
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		panic(fmt.Sprintf("error fetching %s: %s", url, resp.Status))
	}

	// TODO: this is loading the whole file into memory. Steaming would be better.
	body, _ := io.ReadAll(resp.Body)
	lines := strings.Split(string(body), "\n")
	for _, line := range lines {
		for _, word := range strings.Fields(line) {
			strippedWord := removePunctuation(word)
			matched, _ := regexp.Match(`^[a-zA-Z]+$`, []byte(strippedWord))
			if matched {
				frequency.Count(strippedWord)
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
