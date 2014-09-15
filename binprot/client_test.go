package binprot

import (
	"os"
	"os/signal"
	"runtime/pprof"
	//	"github.com/HouzuoGuo/tiedot/data"
	"github.com/HouzuoGuo/tiedot/db"
	"testing"
	"time"
)

const (
	WS = "/tmp/tiedot_binprot_test"
)

func DumpGoroutineOnInterrupt() {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		for _ = range c {
			pprof.Lookup("goroutine").WriteTo(os.Stderr, 1)
		}
	}()
}

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
	// Ping both clients during maintenance access
	if err = clients[0].Ping(); err != nil {
		t.Fatal(err)
	} else if err = clients[1].Ping(); err != nil {
		t.Fatal(err)
	}
	// Wait several seconds then ping again
	time.Sleep(2 * time.Second)
	if err = clients[0].Ping(); err != nil {
		t.Fatal(err)
	} else if err = clients[1].Ping(); err != nil {
		t.Fatal(err)
	} else if err = clients[1].LeaveMaint(); err != nil {
		t.Fatal(err)
	} else if err = clients[0].Ping(); err != nil {
		t.Fatal(err)
	}
	// Shutdown - and it should be harmless to shutdown server/client multiple times
	clients[0].Shutdown()
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

func TestSchemaLookup(t *testing.T) {
	os.RemoveAll(WS)
	var err error
	os.RemoveAll(WS)
	servers := []*BinProtSrv{NewServer(0, 2, WS), NewServer(1, 2, WS)}
	// Prepare database with one collection and one index
	dbs := [2]*db.DB{}
	dbs[0], err = db.OpenDB(servers[0].dbPath)
	if err != nil {
		t.Fatal(err)
	}
	dbs[1], err = db.OpenDB(servers[1].dbPath)
	if err != nil {
		t.Fatal(err)
	}
	dbs[0].Create("A")
	dbs[0].Use("A").Index([]string{"1"})
	dbs[1].Create("A")
	dbs[1].Use("A").Index([]string{"1"})
	// Run two servers
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
	// Check schema
	if len(servers[0].colLookup) != 1 || len(servers[1].colNameLookup) != 1 ||
		len(servers[0].colNameLookup) != 1 || len(servers[1].colLookup) != 1 ||
		len(servers[0].htLookup) != 1 || len(servers[1].htNameLookup) != 1 ||
		len(servers[0].htNameLookup) != 1 || len(servers[1].htLookup) != 1 {
		t.Fatal(servers[0], servers[1])
	}
	if len(clients[0].colLookup) != 1 || len(clients[1].colNameLookup) != 1 ||
		len(clients[0].colNameLookup) != 1 || len(clients[1].colLookup) != 1 ||
		len(clients[0].htLookup) != 1 || len(clients[1].htNameLookup) != 1 ||
		len(clients[0].htNameLookup) != 1 || len(clients[1].htLookup) != 1 {
		t.Fatal(clients[0], clients[1])
	}
	// Simulate a server maintenance event
	dbs[0].Create("B")
	dbs[0].Use("B").Index([]string{"2"})
	dbs[1].Create("B")
	dbs[1].Use("B").Index([]string{"2"})
	if _, err = clients[0].GoMaint(); err != nil {
		t.Fatal(err)
	} else if err = clients[0].LeaveMaint(); err != nil {
		t.Fatal(err)
	}
	// Client should reload schema on the next ping
	if err = clients[0].Ping(); err != nil {
		t.Fatal(err)
	} else if err = clients[1].Ping(); err != nil {
		t.Fatal(err)
	}
	// Check schema
	if len(servers[0].colLookup) != 2 || len(servers[1].colNameLookup) != 2 ||
		len(servers[0].colNameLookup) != 2 || len(servers[1].colLookup) != 2 ||
		len(servers[0].htLookup) != 2 || len(servers[1].htNameLookup) != 2 ||
		len(servers[0].htNameLookup) != 2 || len(servers[1].htLookup) != 2 {
		t.Fatal(servers[0], servers[1])
	}
	if len(clients[0].colLookup) != 2 || len(clients[1].colNameLookup) != 2 ||
		len(clients[0].colNameLookup) != 2 || len(clients[1].colLookup) != 2 ||
		len(clients[0].htLookup) != 2 || len(clients[1].htNameLookup) != 2 ||
		len(clients[0].htNameLookup) != 2 || len(clients[1].htLookup) != 2 {
		t.Fatal(clients[0], clients[1])
	}
	if servers[0].colLookup[servers[0].colNameLookup["A"]] == nil ||
		servers[0].colLookup[servers[0].colNameLookup["B"]] == nil ||
		servers[1].colLookup[servers[1].colNameLookup["A"]] == nil ||
		servers[1].colLookup[servers[1].colNameLookup["B"]] == nil {
		t.Fatal(servers[0], servers[1])
	}
	if clients[0].colLookup[clients[0].colNameLookup["A"]] == nil ||
		clients[0].colLookup[clients[0].colNameLookup["B"]] == nil ||
		clients[1].colLookup[clients[1].colNameLookup["A"]] == nil ||
		clients[1].colLookup[clients[1].colNameLookup["B"]] == nil {
		t.Fatal(clients[0], clients[1])
	}
	if servers[0].htLookup[servers[0].htNameLookup["1"]] == nil ||
		servers[0].htLookup[servers[0].htNameLookup["2"]] == nil ||
		servers[1].htLookup[servers[1].htNameLookup["1"]] == nil ||
		servers[1].htLookup[servers[1].htNameLookup["2"]] == nil {
		t.Fatal(servers[0], servers[1])
	}
	if clients[0].htLookup[clients[0].htNameLookup["1"]] == nil ||
		clients[0].htLookup[clients[0].htNameLookup["2"]] == nil ||
		clients[1].htLookup[clients[1].htNameLookup["1"]] == nil ||
		clients[1].htLookup[clients[1].htNameLookup["2"]] == nil {
		t.Fatal(clients[0], clients[1])
	}
	clients[0].Shutdown()
	clients[1].Shutdown()
	servers[0].Shutdown()
	servers[1].Shutdown()
	clients[0].Close()
	clients[1].Close()
}
