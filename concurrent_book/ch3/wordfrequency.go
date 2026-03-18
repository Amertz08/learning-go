package main

import (
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strings"
	"time"
	"unicode"
)

func main() {
	var frequency = make(map[string]int)

	for i := 1000; i <= 1030; i++ {
		url := fmt.Sprintf("https://rfc-editor.org/rfc/rfc%d.txt", i)
		countWords(url, frequency)
	}
	time.Sleep(10 * time.Second)
	for word, count := range frequency {
		fmt.Printf("%s: %d\n", word, count)
	}
}

func countWords(url string, frequency map[string]int) {
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
				frequency[strings.ToLower(strippedWord)]++
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
