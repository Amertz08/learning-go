package main

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"regexp"
)

func main() {
	fmt.Println(os.Args)
	if len(os.Args) == 1 {
		fmt.Println("no URL provided")
		os.Exit(1)
	}
	url := os.Args[1]
	fmt.Println(url)

	if !hasScheme(url) {
		url = "http://" + url
	}

	resp, err := http.Get(url)
	if err != nil {
		fmt.Println(err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(string(body))
}

func hasScheme(url string) bool {
	matched, _ := regexp.MatchString("^http://.*$", url)
	return matched
}
