package main

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
)

// Ensures gofmt doesn't remove the "net" and "os" imports in stage 1 (feel free to remove this!)
var _ = net.Listen
var _ = os.Exit

type RESP struct {
	Type  byte
	Data  []byte
	Count int
}

const (
	SimpleString byte = '+'
	Error        byte = '-'
	Integer      byte = ':'
	BulkString   byte = '$'
	Array        byte = '*'
)

var store = make(map[string]string)

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
			os.Exit(1)
		}
		go handleRequest(conn)
	}
}

func handleRequest(conn net.Conn) {
	defer conn.Close()
	for {
		buf := make([]byte, 1024)
		_, err := conn.Read(buf)
		if err != nil {
			continue
		}
		_, array := ParseRESP(buf)
		if array.Type != Array {
			conn.Write([]byte("-ERR unknown command\r\n"))
			continue
		}
		resps := make([]RESP, array.Count)
		data := array.Data
		for i := 0; i < array.Count; i++ {
			ln, resp := ParseRESP(data)
			resps[i] = resp
			data = data[ln:]
		}
		cmd := string(resps[0].Data)
		cmd = strings.ToUpper(cmd)
		switch cmd {
		case "PING":
			conn.Write([]byte("+PONG\r\n"))
		case "ECHO":
			if array.Count < 2 {
				conn.Write([]byte("$0\r\n\r\n"))
				continue
			}
			rez := string(resps[1].Data)
			for i := 2; i < array.Count; i++ {
				rez = rez + " " + string(resps[i].Data)
			}
			rez = fmt.Sprintf("$%d\r\n%s\r\n", len(rez), rez)
			conn.Write([]byte(rez))
		case "SET":
			if array.Count < 3 {
				conn.Write([]byte("-ERR wrong number of arguments for 'set' command\r\n"))
				continue
			}
			store[string(resps[1].Data)] = string(resps[2].Data)
			conn.Write([]byte("+OK\r\n"))

		default:
			conn.Write([]byte("-ERR unknown command\r\n"))
		}
	}
}

func ParseRESP(buf []byte) (ln int, resp RESP) {
	if len(buf) == 0 {
		return 0, RESP{}
	}
	resp.Type = buf[0]
	n := 0
	for ; n < len(buf); n++ {
		if buf[n] == '\n' {
			n++
			break
		}
	}
	var err error
	resp.Count, err = strconv.Atoi(string(buf[1 : n-2]))
	if err != nil {
		return 0, RESP{}
	}
	if resp.Type == Array {
		resp.Data = buf[n:]
		return len(buf), resp
	}
	resp.Data = buf[n : n+resp.Count]
	return n + resp.Count + 2, resp

}
