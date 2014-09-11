package binprot

import (
	"os"
	"testing"
)

const (
	WS = "/tmp/tiedot_binprot_test"
)

func TestTwoClientPingMaintShutdown(t *testing.T) {
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
	if err = clients[0].GoMaint(); err != nil {
		t.Fatal(err)
	} else if err = clients[1].GoMaint(); err == nil {
		t.Fatal("did not error")
	} else if err = clients[0].GoMaint(); err == nil {
		t.Fatal("did not error")
	} else if err = clients[0].LeaveMaint(); err != nil {
		t.Fatal(err)
	} else if err = clients[1].GoMaint(); err != nil {
		t.Fatal(err)
	}
	// Shutdown
	clients[0].Shutdown()
	clients[1].Shutdown()
	servers[0].Shutdown()
	servers[1].Shutdown()
}
