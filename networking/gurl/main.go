package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"regexp"
	"sync"
	"time"
)

/*
TODO
	- Ability to send concurrent HTTP requests
	- Testing
*/

type getOptions struct {
	timeout int
}

func main() {
	getCmd := flag.NewFlagSet("get", flag.ExitOnError)
	getOpts := getOptions{}
	getCmd.IntVar(&getOpts.timeout, "timeout", 0, "sets HTTP timeout")

	lookupCmd := flag.NewFlagSet("lookup", flag.ExitOnError)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if len(os.Args) < 3 {
		fmt.Println("expected get|lookup <url>")
		os.Exit(1)
	}
	cmd := os.Args[1]
	url := os.Args[2]

	switch cmd {
	case "get":
		getCmd.Parse(os.Args[3:])
		if err := httpGet(ctx, url, getOpts); err != nil {
			fmt.Println("hit error", err)
			os.Exit(1)
		}
	case "lookup":
		lookupCmd.Parse(os.Args[3:])
		if err := lookupNet(ctx, url); err != nil {
			fmt.Println("lookup error", err)
			os.Exit(1)
		}
	default:
		fmt.Println("invalid command", cmd)
		os.Exit(1)
	}
}

func httpGet(ctx context.Context, url string, opts getOptions) error {
	/*
		TODO
			- header support
			- can download as a file
			- max redirects
	*/
	client := &http.Client{
		Timeout: time.Duration(opts.timeout) * time.Second,
	}

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
