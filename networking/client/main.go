package main

import (
	"fmt"
	"log"
	"net"
)

func main() {

	var msg string
	for {
		conn, err := net.Dial("tcp", ":8080")
		if err != nil {
			log.Fatalf("encountered an error trying to connect: %s", err)
		}
		defer conn.Close()
		fmt.Print("what to send?: ")
		fmt.Scanf("%s", &msg)
		conn.Write([]byte(msg))

	}

}
