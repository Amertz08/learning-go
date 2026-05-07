package main

import (
	"fmt"
	"net"
	"os"
	"sync"
)

func main() {
	workerCount := 5
	port := 8080

	server, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		fmt.Println("error starting server", err)
		os.Exit(1)
	}
	defer server.Close()

	var wg sync.WaitGroup
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func() {
			// TODO: the way this is written goroutines will end after 'workerCount' errors.
			// 		We want the goroutines to be a pool that operations on the connection and regardless
			//		of outcome stay alive. I think we might need to move the accepting/closing of the connection
			// 		into a different goroutine or back into the main goroutine and push the connections over a channel.
			defer wg.Done()
			conn, threadErr := server.Accept()
			if threadErr != nil {
				fmt.Println("error establishing connection", err)
				return
			}
			defer conn.Close()

			buff := make([]byte, 1024)
			size, threadErr := conn.Read(buff)
			if threadErr != nil {
				fmt.Println("error reading buffer")
				return
			}
			msg := buff[:size]
			fmt.Printf("Got: %s\n", msg)

			_, threadErr = conn.Write(buff)
			if threadErr != nil {
				fmt.Println("error writing back to client", err)
				return
			}
		}()
	}
}
