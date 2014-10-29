package binprot

import (
	"fmt"
	"github.com/HouzuoGuo/tiedot/db"
	"os"
	"os/signal"
	"path"
	"runtime/pprof"
	"strconv"
	"testing"
	"time"
)

var WS string = "/tmp/tiedot_binprot_test" + strconv.FormatUint(uint64(time.Now().UnixNano()), 10)

func dumpGoroutineOnInterrupt() {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		for _ = range c {
			pprof.Lookup("goroutine").WriteTo(os.Stderr, 1)
		}
	}()
}

func mkServersClients(n int) (servers []*BinProtSrv, clients []*BinProtClient) {
	servers = make([]*BinProtSrv, n)
	clients = make([]*BinProtClient, n)
	for i := 0; i < n; i++ {
		servers[i] = NewServer(i, n, WS)
		go func(i int) {
			if err := servers[i].Run(); err != nil {
				panic(err)
			}
		}(i)
	}
	for i := 0; i < n; i++ {
		var err error
		if clients[i], err = NewClient(WS); err != nil {
			panic(err)
		}
	}
	return
}

func TestPingBench(t *testing.T) {
	return
	os.RemoveAll(WS)
	defer os.RemoveAll(WS)
	// Run one server and one client
	_, client := mkServersClients(1)
	total := int64(1000000)
	start := time.Now().UnixNano()
	for i := int64(0); i < total; i++ {
		client[0].Ping()
	}
	end := time.Now().UnixNano()
	t.Log("avg latency ns", (end-start)/total)
	t.Log("throughput/sec", float64(total)/(float64(end-start)/float64(1000000000)))
	client[0].Shutdown()
}

func TestPingMaintShutdown(t *testing.T) {
	os.RemoveAll(WS)
	defer os.RemoveAll(WS)
	var err error
	// Run two servers/clients
	servers, clients := mkServersClients(2)
	// Ping both clients
	if err = clients[0].Ping(); err != nil {
		t.Fatal(err)
	} else if err = clients[1].Ping(); err != nil {
		t.Fatal(err)
	}
	// Maintenance access
	if _, err = clients[0].goMaintTest(); err != nil {
		t.Fatal(err)
	} else if _, err = clients[1].goMaintTest(); err == nil {
		t.Fatal("did not error")
	} else if err = clients[1].reloadServerTest(); err == nil {
		t.Fatal("did not error")
	} else if _, err = clients[0].goMaintTest(); err != nil {
		t.Fatal(err)
	} else if err = clients[0].leaveMaintTest(); err != nil {
		t.Fatal(err)
	} else if _, err = clients[1].goMaintTest(); err != nil {
		t.Fatal(err)
	}
	// Ping both clients during maintenance access, then ask server to reload
	if err = clients[0].Ping(); err != nil {
		t.Fatal(err)
	} else if err = clients[1].Ping(); err != nil {
		t.Fatal(err)
	} else if err = clients[1].reloadServerTest(); err != nil {
		t.Fatal(err)
	} else if err = clients[0].reloadServerTest(); err == nil {
		t.Fatal("did not error")
	}
	// Wait several seconds then ping again
	time.Sleep(2 * time.Second)
	if err = clients[0].Ping(); err != nil {
		t.Fatal(err)
	} else if err = clients[1].Ping(); err != nil {
		t.Fatal(err)
	} else if err = clients[1].leaveMaintTest(); err != nil {
		t.Fatal(err)
	} else if err = clients[0].Ping(); err != nil {
		t.Fatal(err)
	}
	// Shutdown while maintenance op is in progress
	if _, err = clients[1].goMaintTest(); err != nil {
		t.Fatal(err)
	}
	go func() {
		time.Sleep(1 * time.Second)
		if err := clients[1].leaveMaintTest(); err != nil {
			t.Fatal(err)
		}
	}()
	fmt.Println("Client 1 shutdown")
	clients[0].Shutdown()
	if err := clients[0].Ping(); err == nil {
		t.Fatal("did not shutdown")
	} else if err = clients[1].Ping(); err == nil {
		t.Fatal("did not shutdown")
	}
	if !servers[0].shutdown || !servers[1].shutdown {
		t.Fatal("server shutdown flags are not set")
	}
	// Client 1 should realize that servers are down
	fmt.Println("Client 2 will close spontaneously")
	time.Sleep(2 * time.Second)
	fmt.Println("Client 2 explicit")
	clients[1].Shutdown()
	fmt.Println("Clients close")
	clients[0].Close()
	clients[1].Close()
	fmt.Println("Servers close")
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
	defer os.RemoveAll(WS)
	var err error
	// Prepare database with one collection and one index
	dbs := [2]*db.DB{}
	dbs[0], err = db.OpenDB(path.Join(WS, "0"))
	if err != nil {
		t.Fatal(err)
	}
	dbs[1], err = db.OpenDB(path.Join(WS, "1"))
	if err != nil {
		t.Fatal(err)
	}
	if err = dbs[0].Create("A"); err != nil {
		t.Fatal(err)
	} else if err = dbs[0].Use("A").Index([]string{"1"}); err != nil {
		t.Fatal(err)
	} else if err = dbs[1].Create("A"); err != nil {
		t.Fatal(err)
	} else if err = dbs[1].Use("A").Index([]string{"1"}); err != nil {
		t.Fatal(err)
	}
	// Run two servers/clients
	servers, clients := mkServersClients(2)
	// Check schema
	if len(servers[0].schema.colLookup) != 1 || len(servers[1].schema.colLookup) != 1 || len(servers[0].schema.htLookup) != 1 || len(servers[1].schema.htLookup) != 1 {
		t.Fatal(servers[0], servers[1])
	}
	if len(clients[0].schema.colLookup) != 1 || len(clients[1].schema.colLookup) != 1 ||
		len(clients[0].schema.htLookup) != 1 || len(clients[1].schema.htLookup) != 1 {
		t.Fatal(clients[0], clients[1])
	}
	// Simulate a server maintenance event
	dbs[0].Create("B")
	dbs[0].Use("B").Index([]string{"2"})
	dbs[1].Create("B")
	dbs[1].Use("B").Index([]string{"2"})
	if _, err = clients[0].goMaintTest(); err != nil {
		t.Fatal(err)
	} else if err = clients[0].leaveMaintTest(); err != nil {
		t.Fatal(err)
	}
	// Client should reload schema on the next ping
	if err = clients[0].Ping(); err != nil {
		t.Fatal(err)
	} else if err = clients[1].Ping(); err != nil {
		t.Fatal(err)
	}
	// Try reload again
	if _, err = clients[1].goMaintTest(); err != nil {
		t.Fatal(err)
	} else if err = clients[1].reloadServerTest(); err != nil {
		t.Fatal(err)
	} else if err = clients[1].leaveMaintTest(); err != nil {
		t.Fatal(err)
	}
	// Check schema
	if len(servers[0].schema.colLookup) != 2 || len(servers[1].schema.colLookup) != 2 ||
		len(servers[0].schema.htLookup) != 2 || len(servers[1].schema.htLookup) != 2 {
		t.Fatal(servers[0], servers[1])
	}
	if len(clients[0].schema.colLookup) != 2 || len(clients[1].schema.colNameLookup) != 2 ||
		len(clients[0].schema.colNameLookup) != 2 || len(clients[1].schema.colLookup) != 2 ||
		len(clients[0].schema.htLookup) != 2 || len(clients[1].schema.htLookup) != 2 {
		t.Fatal(clients[0], clients[1])
	}
	for i := 0; i < 2; i++ {
		for htID, idxPathSegs := range clients[i].schema.indexPaths[clients[i].schema.colNameLookup["A"]] {
			if len(idxPathSegs) != 1 || idxPathSegs[0] != "1" {
				t.Fatal(htID, idxPathSegs)
			}
			if servers[i].schema.htLookup[htID] == nil {
				t.Fatal(servers[i].schema.htLookup)
			} else if servers[i].schema.htLookup[htID] == nil {
				t.Fatal(servers[i].schema.htLookup)
			}
		}
		for htID, idxPathSegs := range clients[i].schema.indexPaths[clients[i].schema.colNameLookup["B"]] {
			if len(idxPathSegs) != 1 || idxPathSegs[0] != "2" {
				t.Fatal(htID, idxPathSegs)
			}
			if servers[i].schema.htLookup[htID] == nil {
				t.Fatal(servers[i].schema.htLookup)
			} else if servers[i].schema.htLookup[htID] == nil {
				t.Fatal(servers[i].schema.htLookup)
			}
		}
	}
	clients[0].Shutdown()
	clients[1].Shutdown()
	servers[0].Shutdown()
	servers[1].Shutdown()
	clients[0].Close()
	clients[1].Close()
}
