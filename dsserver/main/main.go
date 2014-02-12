package main

import (
	"github.com/HouzuoGuo/tiedot/dsserver/dstruct"
	"github.com/HouzuoGuo/tiedot/dsserver/srv"
	"math/rand"
	"net"
	"net/rpc"
	"time"
	"fmt"
	"strconv"
)

func randport() string {
	return ":" + strconv.Itoa(rand.Intn(60000))

}

func main() {
	// Run server
	rand.Seed(int64(time.Now().UnixNano()))
	port := randport()
	server := &srv.RpcServer{Htables: make(map[string]*dstruct.HashTable)}
	rpc.Register(server)
	sock, err := net.Listen("tcp", port)
	if err != nil {
		panic(err)
	}
	// Serve clients
	go func() {
		for {
			conn, err := sock.Accept()
			if err != nil {
				panic(err)
			}
			go rpc.ServeConn(conn)
		}
	}()
	// Run client
	client, err := rpc.Dial("tcp", "127.0.0.1"+port)
	if err != nil {
		panic(err)
	}
	var resp bool
	// Open a hash table
	if err = client.Call("RpcServer.Hopen", &srv.HashOpen{"/tmp/hashtable" + strconv.Itoa(int(time.Now().UnixNano())), "a"}, &resp); err != nil {
		panic(err)
	}
	// Put 1 million entries
	start := time.Now().UnixNano()
	for i := 0; i < 100000; i++ {
		if err = client.Call("RpcServer.Hset", &srv.HashReq{IndexName: "a", Key: uint64(rand.Int()), Val: uint64(rand.Int())}, &resp); err != nil {
			panic(err)
		}
	}
	fmt.Println(time.Now().UnixNano() - start)
	client.Close()
}
