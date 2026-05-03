package main

import (
	"errors"
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
	if err := lookupNet(url); err != nil {
		fmt.Println("hit error", err)
		os.Exit(1)
	}
}

// lookupNet makes some calls in the root 'net' package
func lookupNet(url string) error {
	addresses, err := net.LookupHost(url)
	if err != nil {
		fmt.Println(err)
		return errors.New("failed host lookup")
	}

	results := make(chan addrLookupResult)

	var wg sync.WaitGroup
	for _, addr := range addresses {
		wg.Add(1)
		go func(a string) {
			defer wg.Done()
			name, e := net.LookupAddr(a)
			lookup := addrLookupResult{addr: a, names: name, err: e}
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
		return errors.New("failed CNAME lookup")
	}
	fmt.Println(cname)
	txt, err := net.LookupTXT(url)
	if err != nil {
		fmt.Println(err)
		return errors.New("failed TXT lookup")
	}
	for _, t := range txt {
		fmt.Println(t)
	}
	return nil
}

type addrLookupResult struct {
	addr  string
	names []string
	err   error
}

func hasScheme(url string) bool {
	matched, _ := regexp.MatchString("^http://|https://.*$", url)
	return matched
}
