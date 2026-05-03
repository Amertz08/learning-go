package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"regexp"
	"sync"
)

/*
TODO
	- CLI interface
	- Ability to send concurrent HTTP requests
	- Testing
*/

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

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
	if err := httpGet(ctx, url); err != nil {
		fmt.Println("hit error", err)
		os.Exit(1)
	}
}

func httpGet(ctx context.Context, url string) error {
	/*
		TODO
			- header support
			- can download as a file
			- timeout support
			- max redirects
	*/
	client := &http.Client{}

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return errors.New("error creating request")
	}

	resp, err := client.Do(req)
	if err != nil {
		return errors.New("error sending request")
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return errors.New("error reading response body")
	}
	fmt.Println(string(body))

	return nil
}

// lookupNet makes some calls in the root 'net' package
func lookupNet(ctx context.Context, url string) error {
	// TODO: print prettier
	addresses, err := net.LookupHost(url)
	if err != nil {
		fmt.Println(err)
		return errors.New("failed host lookup")
	}

	results := make(chan addrLookupResult)

	var wg sync.WaitGroup
	for _, addr := range addresses {
		wg.Add(1)
		go func(ctx context.Context, a string) {
			defer wg.Done()
			name, e := net.LookupAddr(a)
			lookup := addrLookupResult{addr: a, names: name, err: e}
			select {
			case results <- lookup:
			case <-ctx.Done():
				return
			}
			results <- lookup
		}(ctx, addr)

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
