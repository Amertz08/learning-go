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
	"strings"
	"sync"
	"time"
)

/*
	Purpose of this app is to learn the `net` package and some of it's sub packages as well as `flag`
TODO
	- Ability to send concurrent HTTP requests
	- Testing
*/

const (
	getCommand    = "get"
	lookupCommand = "lookup"
)

type headerFlags map[string]string

func (h *headerFlags) String() string {
	return fmt.Sprintf("%v", *h)
}

func (h *headerFlags) Set(value string) error {
	kv := strings.SplitN(value, "=", 2)
	if len(kv) != 2 {
		return fmt.Errorf("invalid format, use key=value")
	}
	(*h)[kv[0]] = kv[1]
	return nil
}

type getOptions struct {
	timeout      int
	headers      headerFlags
	maxRedirects int
	fileName     string
}

func main() {
	getCmd := flag.NewFlagSet(getCommand, flag.ExitOnError)
	getOpts := getOptions{}
	getCmd.IntVar(&getOpts.timeout, "timeout", 0, "sets HTTP timeout in milliseconds")
	getCmd.Var(&getOpts.headers, "header", "sets headers")
	getCmd.IntVar(
		&getOpts.maxRedirects,
		"max-redirects",
		-1,
		"sets max redirects: 0 will disable redirects",
	)
	getCmd.StringVar(&getOpts.fileName, "output", "", "downloads as file")

	lookupCmd := flag.NewFlagSet(lookupCommand, flag.ExitOnError)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if len(os.Args) < 3 {
		fmt.Println("expected get|lookup <url>")
		os.Exit(1)
	}
	cmd := os.Args[1]
	url := os.Args[2]

	switch cmd {
	case getCommand:
		getCmd.Parse(os.Args[3:])
		if err := httpGet(ctx, url, getOpts); err != nil {
			fmt.Println("hit error", err)
			os.Exit(1)
		}
	case lookupCommand:
		lookupCmd.Parse(os.Args[3:])
		if err := lookupNet(ctx, url); err != nil {
			fmt.Println("lookup error", err)
			os.Exit(1)
		}
	default:
		fmt.Println("invalid command. expected get|lookup", cmd)
		os.Exit(1)
	}
}

// httpGet supports HTTP get requests with assorted options
func httpGet(ctx context.Context, url string, opts getOptions) error {
	/*
		TODO:
			- cookie support
	*/
	client := &http.Client{
		Timeout: time.Duration(opts.timeout) * time.Millisecond,
	}
	if opts.maxRedirects == 0 {
		client.CheckRedirect = func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		}
	}
	if opts.maxRedirects > 0 {
		client.CheckRedirect = func(req *http.Request, via []*http.Request) error {
			if len(via) >= opts.maxRedirects {
				return errors.New(fmt.Sprintf("stopped after %d redirects", opts.maxRedirects))
			}
			return nil
		}
	}

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return errors.New("error creating request")
	}
	for k, v := range opts.headers {
		req.Header.Set(k, v)
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
	if opts.fileName != "" {
		f, err := os.Open(opts.fileName)
		if err != nil {
			return errors.New(fmt.Sprintf("error opening file: %s", opts.fileName))
		}
		defer f.Close()

		_, err = f.Write(body)
		if err != nil {
			return errors.New(fmt.Sprintf("error writing file: %s", opts.fileName))
		}
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
