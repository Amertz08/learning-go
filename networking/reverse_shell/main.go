package main

import (
	"fmt"
	"log"
	"net"
	"os/exec"
	"strings"
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
			defer c.Close()

			buff := make([]byte, 1024)
			size, _ := c.Read(buff)
			msg := buff[:size]
			fmt.Printf("GOT: %s\n", msg)

			var out strings.Builder
			cmd := exec.Command(string(msg))
			cmd.Stdout = &out
			if cmdErr := cmd.Run(); cmdErr != nil {
				fmt.Println(cmdErr)
			} else {
				fmt.Println(out.String())
			}

		}(conn)
	}
}
