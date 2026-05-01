package main

import (
	"bufio"
	"fmt"
	"net"
	"sync"
)

func main() {
	clientCount := 15

	var wg sync.WaitGroup
	for i := 0; i < clientCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn, err := net.Dial("tcp", "localhost:8080")
			if err != nil {
				fmt.Printf("error connecting client: %s", err)
				return
			}
			defer conn.Close()

			fmt.Fprintf(conn, "Hello from client\n")

			scanner := bufio.NewScanner(conn)
			for scanner.Scan() {
				fmt.Println("received from server:", scanner.Text())
			}
		}()
	}
	wg.Wait()
}
