package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
)

func main() {
	conn, err := net.Dial("tcp", "localhost:8080")
	if err != nil {
		log.Fatalf("error connecting client: %s", err)
	}
	defer conn.Close()

	fmt.Fprintf(conn, "Hello from client\n")

	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		fmt.Println("received from server:", scanner.Text())
	}
}
