package sharding

import (
	"fmt"
	"github.com/HouzuoGuo/tiedot/db"
	"os"
	"os/signal"
	"path"
	"reflect"
	"runtime/pprof"
	"strconv"
	"testing"
	"time"
)

func dumpGoroutineOnInterrupt() {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		for _ = range c {
			pprof.Lookup("goroutine").WriteTo(os.Stderr, 1)
		}
	}()
}

func mkServersClientsReuseWS(ws string, n int) (servers []*ShardServer, clients []*RouterClient) {
	servers = make([]*ShardServer, n)
	clients = make([]*RouterClient, n)
	for i := 0; i < n; i++ {
		servers[i] = NewServer(i, n, ws)
		go func(i int) {
			if err := servers[i].Run(); err != nil {
				panic(err)
			}
		}(i)
	}
	for i := 0; i < n; i++ {
		var err error
		if clients[i], err = NewClient(ws); err != nil {
			panic(err)
		}
	}
	return
}

func mkServersClients(n int) (ws string, servers []*ShardServer, clients []*RouterClient) {
	ws = "/tmp/tiedot_binprot_test" + strconv.FormatUint(uint64(time.Now().UnixNano()), 10)
	os.RemoveAll(ws)
	servers = make([]*ShardServer, n)
	clients = make([]*RouterClient, n)
	for i := 0; i < n; i++ {
		servers[i] = NewServer(i, n, ws)
		go func(i int) {
			if err := servers[i].Run(); err != nil {
				panic(err)
			}
		}(i)
	}
	for i := 0; i < n; i++ {
		var err error
		if clients[i], err = NewClient(ws); err != nil {
			panic(err)
		}
	}
	return
}

func TestPingBench(t *testing.T) {
	return
	// Run one server and one client
	ws, _, clients := mkServersClients(1)
	defer os.RemoveAll(ws)
	total := int64(1000000)
	start := time.Now().UnixNano()
	for i := int64(0); i < total; i++ {
		clients[0].Ping()
	}
	end := time.Now().UnixNano()
	t.Log("avg latency ns", (end-start)/total)
	t.Log("throughput/sec", float64(total)/(float64(end-start)/float64(1000000000)))
	clients[0].Shutdown()
}

func TestPingMaintShutdown(t *testing.T) {
	ws, servers, clients := mkServersClients(2)
	defer os.RemoveAll(ws)
	var err error
	// Run two servers/clients
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
	// Should not panic
	if err = clients[0].Ping(); err == nil {
		t.Fatal("did not shutdown")
	} else if err = clients[1].Ping(); err == nil {
		t.Fatal("did not shutdown")
	}
}

func schemaIdentical(s1 *Schema, s2 *Schema, numCols, numHTs int) error {
	// rev
	if s1.rev != s2.rev {
		return fmt.Errorf("rev mismatch %d - %d", s1.rev, s2.rev)
	}
	// colLookup
	if len(s1.colLookup) != len(s2.colLookup) {
		return fmt.Errorf("colLookup %v - %v", s1.colLookup, s2.colLookup)
	}
	for id, _ := range s1.colLookup {
		if _, exists := s2.colLookup[id]; !exists {
			return fmt.Errorf("colLookup id %d mismatch", id)
		}
	}
	// colNameLookup
	s1ColNameLookup := fmt.Sprint(s1.colNameLookup)
	s2ColNameLookup := fmt.Sprint(s2.colNameLookup)
	if !reflect.DeepEqual(s1.colNameLookup, s2.colNameLookup) {
		return fmt.Errorf("colNameLookup %v - %v", s1ColNameLookup, s2ColNameLookup)
	}
	if len(s1.colNameLookup) != numCols || len(s1.colLookup) != numCols {
		return fmt.Errorf("col count mismatch")
	}
	// htLookup
	if len(s1.htLookup) != len(s2.htLookup) {
		return fmt.Errorf("htLookup %v - %v", s1.htLookup, s2.htLookup)
	}
	for id, _ := range s1.htLookup {
		if _, exists := s2.htLookup[id]; !exists {
			return fmt.Errorf("htLookup id %d mismatch", id)
		}
	}
	// indexPaths
	s1IndexPaths := fmt.Sprint(s1.indexPaths)
	s2IndexPaths := fmt.Sprint(s2.indexPaths)
	if !reflect.DeepEqual(s1.indexPaths, s2.indexPaths) {
		return fmt.Errorf("indexPaths %v - %v", s1IndexPaths, s2IndexPaths)
	}
	if len(s1.indexPaths) != numCols || len(s2.indexPaths) != numCols {
		return fmt.Errorf("col count mismatch")
	}
	// indexPathsJoint
	s1IndexPathsJoint := fmt.Sprint(s1.indexPathsJoint)
	s2IndexPathsJoint := fmt.Sprint(s2.indexPathsJoint)
	if !reflect.DeepEqual(s1.indexPathsJoint, s2.indexPathsJoint) {
		return fmt.Errorf("indexPathsJoint %v - %v", s1IndexPathsJoint, s2IndexPathsJoint)
	}
	if len(s1.indexPathsJoint) != numCols || len(s2.indexPathsJoint) != numCols {
		return fmt.Errorf("col count mismatch")
	}
	return nil
}

func TestSchemaLookup(t *testing.T) {
	ws := "/tmp/tiedot_binprot_test" + strconv.FormatUint(uint64(time.Now().UnixNano()), 10)
	defer os.RemoveAll(ws)
	var err error
	// Prepare database with a collection A and index "1"
	dbs := [2]*db.DB{}
	dbs[0], err = db.OpenDB(path.Join(ws, "0"))
	if err != nil {
		t.Fatal(err)
	}
	dbs[1], err = db.OpenDB(path.Join(ws, "1"))
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
	servers, clients := mkServersClientsReuseWS(ws, 2)
	// Check schema
	if len(servers[0].schema.colLookup) != 1 || len(servers[1].schema.colLookup) != 1 || len(servers[0].schema.htLookup) != 1 || len(servers[1].schema.htLookup) != 1 {
		t.Fatal(servers[0], servers[1])
	}
	if len(clients[0].schema.colLookup) != 1 || len(clients[1].schema.colLookup) != 1 ||
		len(clients[0].schema.htLookup) != 1 || len(clients[1].schema.htLookup) != 1 {
		t.Fatal(clients[0], clients[1])
	}
	// Simulate a server maintenance event - create a collection B with an index "2"
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
	// Check consistency between server and client schema
	if err := schemaIdentical(servers[0].schema, servers[1].schema, 2, 2); err != nil {
		t.Fatal(err)
	}
	if err := schemaIdentical(servers[0].schema, clients[0].schema, 2, 2); err != nil {
		t.Fatal(err)
	}
	if err := schemaIdentical(clients[0].schema, clients[1].schema, 2, 2); err != nil {
		t.Fatal(err)
	}
	fmt.Println("Manual reload")
	// Try reload again
	if _, err = clients[1].goMaintTest(); err != nil {
		t.Fatal(err)
	} else if err = clients[1].reloadServerTest(); err != nil {
		t.Fatal(err)
	} else if err = clients[1].leaveMaintTest(); err != nil {
		t.Fatal(err)
	}
	fmt.Println("Client 0 will ping")
	// Client 1 reloaded and left maintenance mode, which increases server revision twice. Let both clients catch up.
	if err := clients[0].Ping(); err != nil {
		t.Fatal(err)
	} else if err := clients[1].Ping(); err != nil {
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
	// Make sure that both clients and servers see identical schema
	if err := schemaIdentical(servers[0].schema, servers[1].schema, 2, 2); err != nil {
		t.Fatal(err)
	}
	if err := schemaIdentical(servers[0].schema, clients[0].schema, 2, 2); err != nil {
		t.Fatal(err)
	}
	if err := schemaIdentical(clients[0].schema, clients[1].schema, 2, 2); err != nil {
		t.Fatal(err)
	}
	clients[0].Shutdown()
	clients[1].Shutdown()
	servers[0].Shutdown()
	servers[1].Shutdown()
	clients[0].Close()
	clients[1].Close()
	// Should not panic
	if err := clients[0].Ping(); err == nil {
		t.Fatal("did not error")
	} else if err := clients[1].Ping(); err == nil {
		t.Fatal("did not error")
	}
}
