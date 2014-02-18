package main

import (
	"github.com/HouzuoGuo/tiedot/dsserver/srv"
	"time"
)

func main() {
	go func() {
		srv.NewServer(0, 2, "/tmp", "/tmp")
	}()
	go func() {
		srv.NewServer(1, 2, "/tmp", "/tmp")
	}()
	time.Sleep(4 * time.Second)
}
