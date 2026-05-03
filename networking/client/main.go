package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
)

func main() {

	var msg string
	for {
		fmt.Print("what to send?: ")
		inputScanner := bufio.NewScanner(os.Stdin)

		conn, err := net.Dial("tcp", ":8080")
		if err != nil {
			log.Fatalf("encountered an error trying to connect: %s", err)
		}

		if inputScanner.Scan() {
			msg = inputScanner.Text()
			msg = strings.Trim(msg, " ")
			fmt.Println("input: ", msg)
			conn.Write([]byte(msg))
		}

		responseScanner := bufio.NewScanner(conn)
		for responseScanner.Scan() {
			fmt.Println(responseScanner.Text())
		}
		conn.Close()
	}

}
