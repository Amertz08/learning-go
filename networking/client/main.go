package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"os"
)

func main() {

	var msg string
	for {
		fmt.Print("what to send?: ")
		scanner := bufio.NewScanner(os.Stdin)

		conn, err := net.Dial("tcp", ":8080")
		if err != nil {
			log.Fatalf("encountered an error trying to connect: %s", err)
		}
		defer conn.Close()

		if scanner.Scan() {
			msg = scanner.Text()
			fmt.Printf("input: %s\n", msg)
			conn.Write([]byte(msg))
		}
	}

}
