package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"sync"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	workerCount := 5
	port := 8080

	server, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		fmt.Println("error starting server", err)
		os.Exit(1)
	}
	defer server.Close()

	connChan := make(chan net.Conn)

	// Start listening goroutine
	go func(ctx context.Context) {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				conn, connErr := server.Accept()
				if connErr != nil {
					fmt.Println("connection error", connErr)
					continue
				}
				connChan <- conn
			}
		}
	}(ctx)

	var wg sync.WaitGroup
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func(ctx context.Context) {
			defer wg.Done()
			for {
				// TODO: this feels like we're going to have a busy loop.
				select {
				case <-ctx.Done():
					return
				case conn, ok := <-connChan:
					if !ok {
						connChan = nil
						return
					}
					if readErr := echoConn(conn); readErr != nil {
						fmt.Println("error reading connection", readErr)
						continue
					}
				}
			}
		}(ctx)
	}

	wg.Wait()
}

func echoConn(conn net.Conn) error {
	defer conn.Close()

	buff := make([]byte, 1024)
	size, readErr := conn.Read(buff)
	if readErr != nil {
		return readErr
	}

	msg := buff[:size]
	fmt.Printf("GOT: %s\n", msg)

	_, writeErr := conn.Write(msg)
	if writeErr != nil {
		return writeErr
	}
	return nil
}
