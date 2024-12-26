package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
)

// Ensures gofmt doesn't remove the "net" and "os" imports in stage 1 (feel free to remove this!)
var _ = net.Listen
var _ = os.Exit

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	// Uncomment this block to pass the first stage

	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	defer l.Close()
	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			continue
		}
		go handleRequest(conn)
	}
}

func handleRequest(conn net.Conn) {
	defer conn.Close()
	fmt.Println("New connection established")
	reader := bufio.NewReader(conn)
	for {
		cmd, err := reader.ReadString('\n')
		if err != nil {
			fmt.Println("Error reading from connection: ", err.Error())
			return
		}

		fmt.Println("Command received: ", cmd)
		if _, err := conn.Write([]byte("+PONG\r\n")); err != nil {
			fmt.Println("Error writing to connection: ", err.Error())
		}

	}
}
