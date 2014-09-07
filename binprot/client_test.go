package binprot

import (
	"os"
	"testing"
)

const (
	WS = "/tmp/tiedot_binprot_test"
)

func TestTwoClientPingMaintShutdown(t *testing.T) {
	os.RemoveAll(WS)
	srv := NewServer(0, 0, WS)
	go func() {
		if err := srv.Run(); err != nil {
			t.Fatal(err)
		}
	}()
	// Connect two clients
	client1, err := NewClient(WS)
	if err != nil {
		t.Fatal(err)
	}
	client2, err := NewClient(WS)
	if err != nil {
		t.Fatal(err)
	}
	// Ping both clients
	if err = client1.Ping(); err != nil {
		t.Fatal(err)
	}
	if err = client2.Ping(); err != nil {
		t.Fatal(err)
	}
	// Request maintenance mode
	if err = client2.(); err != nil {
		t.Fatal(err)
	}

	client.Shutdown()
	srv.Shutdown()
}
