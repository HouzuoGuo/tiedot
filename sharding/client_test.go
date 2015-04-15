package sharding

import (
	"fmt"
	"github.com/HouzuoGuo/tiedot/data"
	"os"
	"os/signal"
	"reflect"
	"runtime"
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

func mkServersClientsReuseWS(dbdir string, nShards int) (servers []*ShardServer, clients []*RouterClient) {
	runtime.GOMAXPROCS(nShards)
	servers = make([]*ShardServer, nShards)
	clients = make([]*RouterClient, nShards)
	for i := 0; i < nShards; i++ {
		servers[i] = NewServer(i, dbdir)
		go func(i int) {
			if err := servers[i].Run(); err != nil {
				panic(err)
			}
		}(i)
	}
	for i := 0; i < nShards; i++ {
		var err error
		if clients[i], err = NewClient(dbdir); err != nil {
			panic(err)
		}
	}
	return
}

func mkServersClients(nShards int) (dbdir string, servers []*ShardServer, clients []*RouterClient) {
	runtime.GOMAXPROCS(nShards)
	dbdir = "/tmp/tiedot_binprot_test" + strconv.FormatUint(uint64(time.Now().UnixNano()), 10)
	os.RemoveAll(dbdir)
	if err := data.DBNewDir(dbdir, nShards); err != nil {
		panic(err)
	}
	servers = make([]*ShardServer, nShards)
	clients = make([]*RouterClient, nShards)
	for i := 0; i < nShards; i++ {
		servers[i] = NewServer(i, dbdir)
		go func(i int) {
			if err := servers[i].Run(); err != nil {
				panic(err)
			}
		}(i)
	}
	for i := 0; i < nShards; i++ {
		var err error
		if clients[i], err = NewClient(dbdir); err != nil {
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

func schemaIdentical(s1 *data.DBObjects, s2 *data.DBObjects, numCols, numHTs int) error {
	// rev
	if s1.GetCurrentRev() != s2.GetCurrentRev() {
		return fmt.Errorf("rev mismatch %d - %d", s1.GetCurrentRev(), s2.GetCurrentRev())
	}

	// colLookup
	names1 := s1.GetDBFS().GetCollectionNamesSorted()
	names2 := s2.GetDBFS().GetCollectionNamesSorted()
	if !reflect.DeepEqual(names1, names2) {
		return fmt.Errorf("colNames mismatch %v - %v", names1, names2)
	}
	if len(names1) != numCols {
		return fmt.Errorf("Incorrect number of collections - %v", len(names1))
	}
	for _, name := range names1 {
		id1, exists1 := s1.GetColIDByName(name)
		id2, exists2 := s2.GetColIDByName(name)
		if !(exists1 && exists2) || id1 != id2 {
			return fmt.Errorf("colID mismatch - %v %v %v %v", id1, exists1, id2, exists2)
		}
	}

	// htLookup
	htCount1, htCount2 := 0, 0
	for _, name := range names1 {
		colID, _ := s1.GetColIDByName(name)
		allIndexes1, err1 := s1.GetDBFS().GetIndexesSorted(name)
		allIndexes2, err2 := s2.GetDBFS().GetIndexesSorted(name)
		if !reflect.DeepEqual(allIndexes1, allIndexes2) || err1 != nil || err2 != nil {
			return fmt.Errorf("ht mismatch - %v %v", allIndexes1, allIndexes2)
		}
		indexMapping1 := s1.GetIndexesJointPathByColID(colID)
		htCount1 += len(indexMapping1)
		indexMapping2 := s2.GetIndexesJointPathByColID(colID)
		htCount2 += len(indexMapping2)
		if !reflect.DeepEqual(indexMapping1, indexMapping2) {
			return fmt.Errorf("index mismatch %v - %v", indexMapping1, indexMapping2)
		}
		indexPaths1 := s1.GetIndexesByColID(colID)
		indexPaths2 := s2.GetIndexesByColID(colID)
		if !reflect.DeepEqual(indexPaths1, indexPaths2) {
			return fmt.Errorf("index mismatch %v - %v", indexPaths1, indexPaths2)
		}
		if len(indexMapping1) != len(indexPaths1) || len(indexMapping2) != len(indexPaths2) {
			return fmt.Errorf("index mapping mismatch %v - %v, %v - %v", indexMapping1, indexPaths2, indexMapping2, indexPaths2)
		}
	}
	if htCount1 != htCount2 || htCount1 != numHTs {
		return fmt.Errorf("Incorrect number of indexes")
	}
	return nil
}

func TestSchemaLookup(t *testing.T) {
	ws := "/tmp/tiedot_binprot_test" + strconv.FormatUint(uint64(time.Now().UnixNano()), 10)
	defer os.RemoveAll(ws)
	var err error
	// Manually create a DB collection called A and an index on attr "1"
	if err = data.DBNewDir(ws, 2); err != nil {
		t.Fatal(err)
	}
	dbfs, err := data.DBReadDir(ws)
	if err != nil {
		t.Fatal(err)
	} else if err = dbfs.CreateCollection("A"); err != nil {
		t.Fatal(err)
	} else if err = dbfs.CreateIndex("A", "1"); err != nil {
		t.Fatal(err)
	}
	// Run two ordinary servers/clients
	servers, clients := mkServersClientsReuseWS(ws, 2)
	// Check schema
	if err = schemaIdentical(servers[0].dbo, servers[1].dbo, 1, 1); err != nil {
		t.Fatal(err)
	} else if err = schemaIdentical(servers[0].dbo, clients[0].dbo, 1, 1); err != nil {
		t.Fatal(err)
	} else if err = schemaIdentical(clients[0].dbo, clients[1].dbo, 1, 1); err != nil {
		t.Fatal(err)
	}
	// Emulate a server maintenance event - create a collection B with an index "2"
	if err = dbfs.CreateCollection("B"); err != nil {
		t.Fatal(err)
	} else if err = dbfs.CreateIndex("B", "2"); err != nil {
		t.Fatal(err)
	}

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
	if err = schemaIdentical(servers[0].dbo, servers[1].dbo, 2, 2); err != nil {
		t.Fatal(err)
	} else if err = schemaIdentical(servers[0].dbo, clients[0].dbo, 2, 2); err != nil {
		t.Fatal(err)
	} else if err = schemaIdentical(clients[0].dbo, clients[1].dbo, 2, 2); err != nil {
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
	// Check schema consistency again
	if err = schemaIdentical(servers[0].dbo, servers[1].dbo, 2, 2); err != nil {
		t.Fatal(err)
	} else if err = schemaIdentical(servers[0].dbo, clients[0].dbo, 2, 2); err != nil {
		t.Fatal(err)
	} else if err = schemaIdentical(clients[0].dbo, clients[1].dbo, 2, 2); err != nil {
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
