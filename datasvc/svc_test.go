package datasvc

import (
	"fmt"
	"net/rpc"
	"os"
	"strings"
	"testing"
	"time"
)

var err error
var discard *bool = new(bool)

// Discard unused RPC output
var svc *DataSvc = NewDataSvc("/tmp/tiedot_svc_test", 1)
var client *rpc.Client

// Run data server test cases orderly
func TestSequence(t *testing.T) {
	// Initialize test server/client
	os.Remove(svc.sockPath)
	go func() {
		if err := svc.Serve(); err != nil {
			t.Fatal(err)
		}
	}()
	time.Sleep(100 * time.Millisecond)
	if client, err = rpc.Dial("unix", svc.sockPath); err != nil {
		t.Fatal(err)
	}
	// Run test sequence
	PingTest(t)
	HTTest(t)
	PartitionTest(t)
	// Shutdown test server/client
	if err = client.Call("DataSvc.Shutdown", false, discard); err == nil || !strings.Contains(fmt.Sprint(err), "unexpected EOF") {
		t.Fatal("Server did not close connection", err)
	}
	if err = client.Call("DataSvc.Ping", false, discard); err == nil {
		t.Fatal("Did not shutdown")
	}
}

func PingTest(t *testing.T) {
	if !(len(svc.ht) == 0 && len(svc.part) == 0 && svc.dataLock != nil && svc.rank == 1 && svc.clientsLock != nil && svc.clients != nil) {
		t.Fatal(svc)
	}
	time.Sleep(100 * time.Millisecond)
	if err = client.Call("DataSvc.Ping", false, discard); err != nil {
		t.Fatal(err)
	}
}
