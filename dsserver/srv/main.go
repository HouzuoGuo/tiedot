package main

import (
	"bufio"
	"github.com/HouzuoGuo/tiedot/dsserver/colpart"
	"github.com/HouzuoGuo/tiedot/dsserver/dstruct"
	"github.com/HouzuoGuo/tiedot/tdlog"
	"math/rand"
	"net"
	"strconv"
	"strings"
	"time"
	"fmt"
)

const (
	// Hash operations
	HOPEN = "HOPEN"
	HREMV = "HREMV"
	HSYNC = "HSYNC"
	HGET  = "HGT"
	HSET  = "HST"
	HDEL  = "HDE"
	// Collection operations
	COPEN = "COPEN"
	CREMV = "CREMV"
	CSYNC = "CSYNC"
	CINS  = "CIN"
	CUPD  = "CUP"
	CDEL  = "CDE"
	// Other (Ops without params must have suffix new-line)
	SYNALL = "SYCALL\n"
	FIN    = "FIN\n"
	PING   = "PING\n"
	PONG   = "PONG\n"
)

// Tasks are queued on the server and executed one by one.
type Task struct {
	Ret chan interface{}   // Task result
	Fun func() interface{} // Task routine
}

// Server state and structures.
type Server struct {
	Rank, TotalRank int                           // Rank of current process; total number of processes
	ColParts        map[string]*colpart.Partition // Collection name -> partition
	Htables         map[string]*dstruct.HashTable // Collection name -> index name -> hash table
	MainLoop        chan *Task
}

// New server.
func NewServer() *Server {
	return &Server{ColParts: make(map[string]*colpart.Partition), Htables: make(map[string]*dstruct.HashTable), MainLoop: make(chan *Task, 100)}
}

// Start the server task worker.
func (server *Server) Run() {
	go func() {
		for {
			task := <-server.MainLoop
			task.Ret <- task.Fun()
		}
	}()
}

// Submit a task to the server and wait till its completion.
func (server *Server) Submit(task *Task) interface{} {
	server.MainLoop <- task
	return <-task.Ret
}

func main() {
	rand.Seed(int64(time.Now().UnixNano()))
	// Prepare server state
	srv := NewServer()
	ht, err := dstruct.OpenHash(strconv.Itoa(rand.Int()), []string{"a"})
	srv.Htables["a"] = ht
	// Server listener
	sockName := strconv.Itoa(rand.Int())
	listener, err := net.Listen("unix", "/tmp/" + sockName)
	if err != nil {
		panic(err)
	}
	// Command loop
	go func() {
		for {
			work := <-srv.MainLoop
			work.Ret <- work.Fun()
		}
	}()
	serverDone := make(chan bool)
	// Server loop
	go func() {
		for {
			client, err := listener.Accept()
			if err != nil {
				panic(err)
			}
			// Handling incoming connection
			go func(client net.Conn) {
				feedback := make(chan interface{})
				tdlog.Printf("Connection established %v", client)
				input := bufio.NewReader(client)
				output := bufio.NewWriter(client)
				for {
					// Every input line is a command
					input, _ := input.ReadString(byte('\n'))
					cmd := strings.Split(input, " ")
//					tdlog.Printf("CMD: [%v]", cmd)
					switch cmd[0] {
					case PING:
						output.WriteString(PONG + "\n")
						output.Flush()
					case HSET:
						output.WriteString(srv.Submit(&Task{Ret: feedback, Fun: func() interface{} {
							return "1"
						}}).(string) + "\n")
						output.Flush()
					case FIN:
						tdlog.Printf("Closing")
						client.Close()
						serverDone <- true
						return
					}
				}
			}(client)
		}
	}()
	// Client connection
	conn, err := net.Dial("unix", "/tmp/" + sockName)
	input := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)
	start := time.Now().UnixNano()
	for i := 0; i < 200000; i++ {
		writer.WriteString(HSET + " a " + strconv.Itoa(rand.Int()) + " " + strconv.Itoa(rand.Int()) + "\n")
		writer.Flush()
		_, err := input.ReadString(byte('\n'))
		if err != nil {
			panic(err)
		}
	}
	end := time.Now().UnixNano()
	fmt.Println(end - start)
	// Client wait
	writer.WriteString(FIN)
	writer.Flush()
	<- serverDone
}
