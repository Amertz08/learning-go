package main

import (
	"fmt"
	"net"
	"sync"
)

func main() {
	clientCount := 500
	port := 8080

	var wg sync.WaitGroup
	for i := 0; i < clientCount; i++ {
		wg.Add(1)
		go func(port int) {
			defer wg.Done()
			conn, err := net.Dial("tcp", fmt.Sprintf(":%d", port))
			if err != nil {
				fmt.Println("failed to connect")
				return
			}
			defer conn.Close()

			// TODO: write more data
			conn.Write([]byte("hello"))
		}(port)
	}
	wg.Wait()
}
