package main

import (
	"fmt"
	"net"
	"os"
	"regexp"
	"sync"
)

func main() {
	fmt.Println(os.Args)
	if len(os.Args) == 1 {
		fmt.Println("no URL provided")
		os.Exit(1)
	}
	url := os.Args[1]
	fmt.Println(url)

	//if !hasScheme(url) {
	//	url = "http://" + url
	//}

	addresses, err := net.LookupHost(url)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	results := make(chan addrLookupResult)

	var wg sync.WaitGroup
	for _, addr := range addresses {
		wg.Add(1)
		go func(a string) {
			defer wg.Done()
			name, _ := net.LookupAddr(a)
			lookup := addrLookupResult{addr: a, names: name}
			results <- lookup
		}(addr)

	}

	go func() {
		wg.Wait()
		close(results)
	}()

	for r := range results {
		fmt.Println(r)
	}
	cname, err := net.LookupCNAME(url)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	fmt.Println(cname)
	txt, err := net.LookupTXT(url)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	for _, t := range txt {
		fmt.Println(t)
	}
}

type addrLookupResult struct {
	addr  string
	names []string
}

func hasScheme(url string) bool {
	matched, _ := regexp.MatchString("^http://|https://.*$", url)
	return matched
}
