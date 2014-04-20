package datasvc

import (
	"fmt"
	"net/rpc"
	"os"
	"strings"
	"testing"
	"time"
)

var discard *bool = new(bool)

func TestServe(t *testing.T) {
	svc := NewDataSvc("/tmp/tiedot_svc_test", 0)
	os.Remove(svc.sockPath) // just for cleanup
	go func() {
		if err := svc.Serve(); err != nil {
			t.Fatal(err)
		}
	}()
	if !(len(svc.ht) == 0 && len(svc.col) == 0 && svc.dataLock != nil && svc.rank == 0 && svc.clientsLock != nil && svc.clients != nil) {
		t.Fatal(svc)
	}
	time.Sleep(100 * time.Millisecond)
	client, err := rpc.Dial("unix", svc.sockPath)
	if err != nil {
		t.Fatal(err)
	}
	if err = client.Call("DataSvc.Ping", false, discard); err != nil {
		t.Fatal(err)
	}
	if err = client.Call("DataSvc.Shutdown", false, discard); err == nil || !strings.Contains(fmt.Sprint(err), "unexpected EOF") {
		t.Fatal("Server did not close connection", err)
	}
	if err = client.Call("DataSvc.Ping", false, discard); err == nil {
		t.Fatal("Did not shutdown")
	}
}
