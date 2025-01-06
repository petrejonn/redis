package main

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
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
	SimpleStringType byte = '+'
	ErrorType        byte = '-'
	IntegerType      byte = ':'
	BulkStringType   byte = '$'
	ArrayType        byte = '*'
)

type Value struct {
	data string
	exp  int
}

var store = make(map[string]Value)

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
		if array.Type != ArrayType {
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
			conn.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(rez), rez)))
		case "SET":
			if array.Count < 3 {
				conn.Write([]byte("-ERR wrong number of arguments for 'set' command\r\n"))
				continue
			}
			exp := 0
			if array.Count > 3 {
				for i := 4; i <= array.Count; i += 2 {
					arg := strings.ToUpper(string(resps[i-1].Data))
					switch arg {
					case "EX":
						exp, err = strconv.Atoi(string(resps[i].Data))
						if err != nil {
							conn.Write([]byte("-ERR value is not an integer or out of range\r\n"))
							continue
						}
						exp = int(time.Now().Add(time.Duration(exp) * time.Second).Unix())
					case "PX":
						exp, err = strconv.Atoi(string(resps[i].Data))
						if err != nil {
							conn.Write([]byte("-ERR value is not an integer or out of range\r\n"))
							continue
						}
						exp = int(time.Now().Add(time.Duration(exp) * time.Millisecond).Unix())
					default:
						conn.Write([]byte("-ERR syntax error\r\n"))
					}
				}
			}
			store[string(resps[1].Data)] = Value{string(resps[2].Data), exp}
			conn.Write([]byte("+OK\r\n"))
		case "GET":
			if array.Count < 2 {
				conn.Write([]byte("-ERR wrong number of arguments for 'get' command\r\n"))
				continue
			}
			val, ok := store[string(resps[1].Data)]
			fmt.Println(store)
			if !ok || (val.exp != 0 && val.exp < int(time.Now().Unix())) {
				conn.Write([]byte("$-1\r\n"))
				continue
			}
			conn.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(val.data), val.data)))
		case "CONFIG":
			if array.Count < 3 {
				conn.Write([]byte("-ERR wrong number of arguments for 'config' command\r\n"))
			}
			arg := strings.ToLower(string(resps[2].Data))
			switch arg {
			case "dir":
				out := ToRESP(ArrayType, []byte("dir"), []byte("/tmp/redis-files"))
				conn.Write(out)
			case "dbfilename":
				out := ToRESP(ArrayType, []byte("dbfilename"), []byte("dump.rdb"))
				conn.Write(out)
			}

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
	if resp.Type == ArrayType {
		resp.Data = buf[n:]
		return len(buf), resp
	}
	resp.Data = buf[n : n+resp.Count]
	return n + resp.Count + 2, resp

}

func ToRESP(typ byte, args ...[]byte) []byte {
	if typ == ArrayType {
		var out []byte
		var count int
		for _, arg := range args {
			v := []byte(fmt.Sprintf("$%d\r\n%s\r\n", len(arg), arg))
			out = append(out, v...)
			count++
		}
		return append([]byte(fmt.Sprintf("*%d\r\n", count)), out...)
	}
	return []byte("")
}
