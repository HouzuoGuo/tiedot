package binprot

import (
	"os"
	"testing"
)

const (
	WS = "/tmp/tiedot_binprot_test"
)

func TestPing(t *testing.T) {
	os.RemoveAll(WS)
	srv := NewServer(0, 0, WS)
	go func() {
		if err := srv.Run(); err != nil {
			t.Fatal(err)
		}
	}()

	client, err := NewClient(WS)
	if err != nil {
		t.Fatal(err)
	}
	if err = client.Ping(); err != nil {
		t.Fatal(err)
	}
	if err = client.PingErr(); err != nil {
		t.Fatal(err)
	}
	client.Shutdown()
	srv.Shutdown()
}
