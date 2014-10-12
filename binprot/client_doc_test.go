package binprot

import (
	"os"
	"testing"
	"time"
)

func TestDocInsertBench(t *testing.T) {
	dumpGoroutineOnInterrupt()
	os.RemoveAll(WS)
	defer os.RemoveAll(WS)
	_, clients := mkServersClients(2)
	if err := clients[0].Create("col"); err != nil {
		t.Fatal(err)
	} else if err := clients[0].Index("col", []string{"a"}); err != nil {
		t.Fatal(err)
	}
	total := int64(1000000)
	start := time.Now().UnixNano()
	for i := int64(0); i < total; i++ {
		if _, err := clients[i%2].Insert("col", map[string]interface{}{"a": i}); err != nil {
			t.Fatal(err)
		}
	}
	end := time.Now().UnixNano()
	t.Log("avg latency ns", (end-start)/total)
	t.Log("throughput/sec", float64(total)/(float64(end-start)/float64(1000000000)))
	clients[0].Shutdown()
}

func TestDocCrud(t *testing.T) {
	dumpGoroutineOnInterrupt()
	os.RemoveAll(WS)
	defer os.RemoveAll(WS)
	_, clients := mkServersClients(2)
	if err := clients[0].Create("col"); err != nil {
		t.Fatal(err)
	}
	id, err := clients[0].Insert("col", map[string]interface{}{"a": 1})
	t.Log(id)
	if err != nil {
		t.Fatal(err)
	} else if doc, err := clients[1].Read("col", id); err != nil || doc["a"].(float64) != 1 {
		t.Fatal(doc, err)
	}
	id, err = clients[1].Insert("col", map[string]interface{}{"b": 2})
	t.Log(id)
	if err != nil {
		t.Fatal(err)
	} else if doc, err := clients[0].Read("col", id); err != nil || doc["b"].(float64) != 2 {
		t.Fatal(doc, err)
	}
	clients[0].Shutdown()
}
