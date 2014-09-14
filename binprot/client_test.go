package binprot

import (
	"fmt"
	"os"
	"os/signal"
	"runtime/pprof"
	"testing"
	"time"
)

const (
	WS = "/tmp/tiedot_binprot_test"
)

func TestPingBench(t *testing.T) {
	//	t.Skip()
	var err error
	os.RemoveAll(WS)
	// Run two servers
	servers := []*BinProtSrv{NewServer(0, 2, WS)}
	go func() {
		if err := servers[0].Run(); err != nil {
			t.Fatal(err)
		}
	}()
	client := &BinProtClient{}
	if client, err = NewClient(WS); err != nil {
		t.Fatal(err)
	}
	total := int64(1000000)
	start := time.Now().UnixNano()
	for i := int64(0); i < total; i++ {
		client.Ping()
	}
	end := time.Now().UnixNano()
	t.Log("avg latency ns", (end-start)/total)
	t.Log("throughput/sec", float64(total)/(float64(end-start)/float64(1000000000)))
	client.Shutdown()
}

func TestPingMaintShutdown(t *testing.T) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		for _ = range c {
			pprof.Lookup("goroutine").WriteTo(os.Stderr, 1)
		}
	}()

	var err error
	os.RemoveAll(WS)
	// Run two servers
	servers := []*BinProtSrv{NewServer(0, 2, WS), NewServer(1, 2, WS)}
	go func() {
		if err := servers[0].Run(); err != nil {
			t.Fatal(err)
		}
	}()
	go func() {
		if err := servers[1].Run(); err != nil {
			t.Fatal(err)
		}
	}()
	// Connect two clients
	clients := [2]*BinProtClient{}
	if clients[0], err = NewClient(WS); err != nil {
		t.Fatal(err)
	} else if clients[1], err = NewClient(WS); err != nil {
		t.Fatal(err)
	}
	// Ping both clients
	if err = clients[0].Ping(); err != nil {
		t.Fatal(err)
	} else if err = clients[1].Ping(); err != nil {
		t.Fatal(err)
	}
	// Maintenance access
	if _, err = clients[0].GoMaint(); err != nil {
		t.Fatal(err)
	} else if _, err = clients[1].GoMaint(); err == nil {
		t.Fatal("did not error")
	} else if _, err = clients[0].GoMaint(); err != nil {
		t.Fatal(err)
	} else if err = clients[0].LeaveMaint(); err != nil {
		t.Fatal(err)
	} else if _, err = clients[1].GoMaint(); err != nil {
		t.Fatal(err)
	}
	// Ping both clients, then leaveMaint
	if err = clients[0].Ping(); err == nil {
		t.Fatal("did not error")
	} else if err = clients[1].Ping(); err != nil {
		t.Fatal(err)
	} else if err = clients[1].LeaveMaint(); err != nil {
		t.Fatal(err)
	} else if err = clients[0].Ping(); err != nil {
		t.Fatal(err)
	}
	// Shutdown - and it should be harmless to shutdown server/client multiple times
	fmt.Println("First shutdown")
	clients[0].Shutdown()
	fmt.Println("First test after shutdown")
	if err = clients[0].Ping(); err == nil {
		t.Fatal("did not shutdown")
	} else if err = clients[1].Ping(); err == nil {
		t.Fatal("did not shutdown")
	}
	clients[1].Shutdown()
	time.Sleep(2 * time.Second)
	clients[0].Close()
	clients[1].Close()
	servers[0].Shutdown()
	servers[1].Shutdown()
	if err = clients[0].Ping(); err == nil {
		t.Fatal("did not shutdown")
	} else if err = clients[1].Ping(); err == nil {
		t.Fatal("did not shutdown")
	}
}
