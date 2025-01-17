package main

import (
	"bytes"
	"encoding/binary"
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

type RDBMetadata struct {
	Flag  byte
	Key   []byte
	Value []byte
}

type DBRecord struct {
	ExFlag    []byte // FD(seconds), FC(milliseconds)
	Ex        []byte
	ValueType byte
	Key       []byte // string encoded
	Value     []byte // string encoded
}

type DB struct {
	Flag              byte
	Index             []byte //size encoded
	HashTableSizeFlag byte
	HashTableSize     []byte //size encoded
	ExpHashTableSize  []byte //sixe encoded
	Records           []DBRecord
}
type RDB struct {
	// Header section
	MagicString []byte
	Version     []byte
	// Metadata section
	MetaDatas []RDBMetadata
	// DB section
	DBs []DB
	// End of file section
	EOFFlag      byte //
	FileChecksum [8]byte
}

var store = make(map[string]Value)
var rdb RDB

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

	initDB()

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
			conn.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(rez), rez)))
		case "SET":
			if array.Count < 3 {
				conn.Write([]byte("-ERR wrong number of arguments for 'set' command\r\n"))
				continue
			}
			if array.Count > 3 {
				for i := 4; i <= array.Count; i += 2 {
					arg := strings.ToUpper(string(resps[i-1].Data))
					switch arg {
					case "EX":
						// TODO: increase hashtable count
						exp64, err := strconv.ParseUint(string(resps[i].Data), 10, 32)
						exp := uint32(exp64)
						if err != nil {
							conn.Write([]byte("-ERR value is not an integer or out of range\r\n"))
							continue
						}
						exp = uint32(time.Now().Add(time.Duration(exp) * time.Second).Unix())
						expBytes := make([]byte, 4)
						binary.LittleEndian.PutUint32(expBytes, exp)
						key := append([]byte{byte(len(resps[1].Data))}, resps[1].Data...)
						value := append([]byte{byte(len(resps[2].Data))}, resps[2].Data...)
						rdb.DBs[0].Records = append(rdb.DBs[0].Records, DBRecord{
							ExFlag:    []byte{0xFD},
							Ex:        expBytes,
							ValueType: byte(0x00),
							Key:       key,
							Value:     value,
						})
					case "PX":
						// TODO: increase hashtable count
						exp, err := strconv.ParseUint(string(resps[i].Data), 10, 64)
						if err != nil {
							conn.Write([]byte("-ERR value is not an integer or out of range\r\n"))
							continue
						}
						exp = uint64(time.Now().Add(time.Duration(exp) * time.Millisecond).Unix())
						expBytes := make([]byte, 8)
						binary.LittleEndian.PutUint64(expBytes, exp)
						key := append([]byte{byte(len(resps[1].Data))}, resps[1].Data...)
						value := append([]byte{byte(len(resps[2].Data))}, resps[2].Data...)
						rdb.DBs[0].Records = append(rdb.DBs[0].Records, DBRecord{
							ExFlag:    []byte{0xFC},
							Ex:        expBytes,
							ValueType: byte(0x00),
							Key:       key,
							Value:     value,
						})
					default:
						conn.Write([]byte("-ERR syntax error\r\n"))
					}
				}
			} else {
				key := append([]byte{byte(len(resps[1].Data))}, resps[1].Data...)
				value := append([]byte{byte(len(resps[2].Data))}, resps[2].Data...)
				rdb.DBs[0].Records = append(rdb.DBs[0].Records, DBRecord{
					ValueType: byte(0x00),
					Key:       key,
					Value:     value,
				})
			}
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
		case "SAVE":
			rdb.save()
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

func initDB() {
	rdb = RDB{
		MagicString: []byte{0x52, 0x45, 0x44, 0x49, 0x53},
		Version:     []byte{0x30, 0x30, 0x31, 0x31},
		MetaDatas: []RDBMetadata{
			{
				Flag:  byte(0xFA),
				Key:   []byte{0x09, 0x72, 0x65, 0x64, 0x69, 0x73, 0x2D, 0x76, 0x65, 0x72},
				Value: []byte{0x06, 0x36, 0x2E, 0x30, 0x2E, 0x31, 0x36},
			},
		},
		DBs: []DB{
			{
				Flag:              byte(0xFE),
				Index:             []byte{0x00},
				HashTableSizeFlag: byte(0xFB),
				HashTableSize:     []byte{0x00},
				ExpHashTableSize:  []byte{0x00},
			},
		},
	}
}

func (r RDB) save() {
	file, err := os.Create("db.rdb")
	if err != nil {
		fmt.Printf("Error creating DB file")
	}
	defer file.Close()
	buffer := new(bytes.Buffer)
	buffer.Write(r.MagicString)
	buffer.Write(r.Version)
	// Metadata section
	for _, metaData := range r.MetaDatas {
		buffer.WriteByte(metaData.Flag)
		buffer.Write(metaData.Key)
		buffer.Write(metaData.Value)
	}
	// DB section
	for _, db := range r.DBs {
		buffer.WriteByte(db.Flag)
		buffer.Write(db.Index)
		buffer.WriteByte(db.HashTableSizeFlag)
		buffer.Write(db.HashTableSize)
		buffer.Write(db.ExpHashTableSize)
		for _, record := range db.Records {
			buffer.Write(record.ExFlag)
			buffer.Write(record.Ex)
			buffer.WriteByte(record.ValueType)
			buffer.Write(record.Key)
			buffer.Write(record.Value)
		}
	}
	file.Write(buffer.Bytes())
}
