package main

import (
	"fmt"
	"net"
)

func main() {
	connections := make(chan net.Conn)
	startWorkers(3, connections)
	server, _ := net.Listen("tcp", "localhost:8080")
	fmt.Println("starting server")
	defer server.Close()
	for {
		conn, _ := server.Accept()
		select {
		case connections <- conn:
		default:
			fmt.Println("Server is busy")
			conn.Write([]byte("too many clients"))
			conn.Close()
		}
	}
}

func startWorkers(count int, connections chan net.Conn) {
	for i := 0; i < count; i++ {
		go func() {
			for c := range connections {
				buff := make([]byte, 1024)
				size, _ := c.Read(buff)
				fmt.Printf("received %s\n", buff[:size])
				c.Write([]byte("Hello from server"))
				c.Close()
			}
		}()
	}
}
