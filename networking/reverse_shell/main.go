package main

import (
	"fmt"
	"log"
	"net"
)

func main() {
	serv, err := net.Listen("tcp", ":8080")
	defer serv.Close()
	if err != nil {
		log.Fatalf("encountered error starting server: %s", err)
	}
	for {
		conn, err := serv.Accept()
		if err != nil {
			log.Fatalf("encountered an error accepting connections: %s", err)
		}
		go func(c net.Conn) {
			buff := make([]byte, 1024)
			size, _ := c.Read(buff)
			fmt.Printf("%s\n", buff[:size])
			c.Close()
		}(conn)
	}
}
