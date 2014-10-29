package binprot

import (
	"os"
	"testing"
	"time"
)

func TestDocInsertBench(t *testing.T) {
	return
	os.RemoveAll(WS)
	defer os.RemoveAll(WS)
	_, clients := mkServersClients(2)
	if err := clients[0].Create("col"); err != nil {
		t.Fatal(err)
	} else if err := clients[0].Index("col", []string{"a"}); err != nil {
		t.Fatal(err)
	} else if err := clients[0].Index("col", []string{"b"}); err != nil {
		t.Fatal(err)
	} else if err := clients[0].Index("col", []string{"c"}); err != nil {
		t.Fatal(err)
	}
	total := int64(100000)
	start := time.Now().UnixNano()
	for i := int64(0); i < total; i++ {
		if _, err := clients[i%2].Insert("col", map[string]interface{}{"a": i, "b": i, "c": i}); err != nil {
			t.Fatal(err)
		}
	}
	end := time.Now().UnixNano()
	t.Log("avg latency ns", (end-start)/total)
	t.Log("throughput/sec", float64(total)/(float64(end-start)/float64(1000000000)))
	clients[0].Shutdown()
}

func TestDocCrud(t *testing.T) {
	var err error
	os.RemoveAll(WS)
	defer os.RemoveAll(WS)
	_, clients := mkServersClients(2)
	if err := clients[0].Create("col"); err != nil {
		t.Fatal(err)
	} else if err = clients[0].Index("col", []string{"a", "b"}); err != nil {
		t.Fatal(err)
	}
	numDocs := 2011
	docIDs := make([]uint64, numDocs)
	// Insert docs
	for i := 0; i < numDocs; i++ {
		if docIDs[i], err = clients[i%2].Insert("col", map[string]interface{}{"a": map[string]interface{}{"b": i}}); err != nil {
			t.Fatal(err)
		}
	}
	// Read documents and verify index
	if _, err = clients[0].Read("col", 123456); err == nil {
		t.Fatal("did not error")
	} else if _, err = clients[1].Read("does not exist", docIDs[0]); err == nil {
		t.Fatal("did not error")
	}
	for i, docID := range docIDs {
		if doc, err := clients[i%2].Read("col", docID); err != nil || doc["a"].(map[string]interface{})["b"].(float64) != float64(i) {
			t.Fatal(docID, doc)
		} else if err = clients[i%2].valIsIndexed("col", []string{"a", "b"}, i, docID); err != nil {
			t.Fatal(err)
		}
	}
	// Update document
	if err = clients[0].Update("col", 654321, map[string]interface{}{}); err == nil {
		t.Fatal("did not error")
	} else if err = clients[1].Update("does not exist", docIDs[0], map[string]interface{}{}); err == nil {
		t.Fatal("did not error")
	}
	for i, docID := range docIDs {
		// i -> i * 2
		if err = clients[i%2].Update("col", docID, map[string]interface{}{"a": map[string]interface{}{"b": i * 2}}); err != nil {
			t.Fatal(i, docID, err)
		}
	}
	// Verify documents and index after update
	for i, docID := range docIDs {
		if doc, err := clients[i%2].Read("col", docID); err != nil || doc["a"].(map[string]interface{})["b"].(float64) != float64(i*2) {
			t.Fatal(docID, doc)
		}
		if i == 0 {
			if err = clients[i%2].valIsIndexed("col", []string{"a", "b"}, 0, docID); err != nil {
				t.Fatal(err)
			}
		} else {
			if err = clients[i%2].valIsNotIndexed("col", []string{"a", "b"}, uint64(i), docID); err != nil {
				t.Fatal(err)
			}
			if err = clients[i%2].valIsIndexed("col", []string{"a", "b"}, uint64(i*2), docID); err != nil {
				t.Fatal(err)
			}
		}
	}
	// Delete half of the documents
	if err = clients[1].Delete("col", 654321); err == nil {
		t.Fatal("did not error")
	} else if err = clients[0].Delete("does not exist", docIDs[0]); err == nil {
		t.Fatal("did not error")
	}
	for i := 0; i < numDocs/2+1; i++ {
		if err := clients[i%1].Delete("col", docIDs[i]); err != nil {
			t.Fatal(err)
		} else if err := clients[i%1].Delete("col", docIDs[i]); err == nil {
			t.Fatal("Did not error")
		}
	}
	// Verify documents and index after delete
	for i, docID := range docIDs {
		if i < numDocs/2+1 {
			// Verify deleted documents and index
			if _, err := clients[i%1].Read("col", docID); err == nil {
				t.Fatal("Did not delete", i, docID)
			}
			if err = clients[i%1].valIsNotIndexed("col", []string{"a", "b"}, uint64(i*2), docID); err != nil {
				t.Fatal(err)
			}
		} else {
			// Verify unaffected documents and index
			if doc, err := clients[i%1].Read("col", docID); err != nil || doc["a"].(map[string]interface{})["b"].(float64) != float64(i*2) {
				t.Fatal(docID, doc)
			}
			if err = clients[i%1].valIsIndexed("col", []string{"a", "b"}, i*2, docID); err != nil {
				t.Fatal(err)
			}
		}
	}
	// Recreate index and verify documents & index
	if err = clients[0].Unindex("col", []string{"a", "b"}); err != nil {
		t.Fatal(err)
	} else if indexes, err := clients[1].AllIndexes("col"); err != nil || len(indexes) != 0 {
		t.Fatal("did not remove index")
	} else if err = clients[1].Index("col", []string{"a", "b"}); err != nil {
		t.Fatal(err)
	}
	for i, docID := range docIDs {
		if i < numDocs/2+1 {
			if _, err := clients[i%1].Read("col", docID); err == nil {
				t.Fatal("Did not delete", i, docID)
			}
			if err = clients[i%1].valIsNotIndexed("col", []string{"a", "b"}, uint64(i*2), docID); err != nil {
				t.Fatal(err)
			}
		} else {
			if doc, err := clients[i%1].Read("col", docID); err != nil || doc["a"].(map[string]interface{})["b"].(float64) != float64(i*2) {
				t.Fatal(docID, doc)
			}
			if err = clients[i%1].valIsIndexed("col", []string{"a", "b"}, i*2, docID); err != nil {
				t.Fatal(err)
			}
		}
	}
	// Verify that there are approximately 1000 documents
	if count, err := clients[0].ApproxDocCount("col"); err != nil {
		t.Fatal(err)
	} else if count < 600 || count > 1400 {
		t.Fatal("Approximate is way off")
	}
	// Scrub and verify documents & index (same verification as the two above)
	if err = clients[1].Scrub("col"); err != nil {
		t.Fatal(err)
	}
	for i, docID := range docIDs {
		if i < numDocs/2+1 {
			if _, err := clients[i%1].Read("col", docID); err == nil {
				t.Fatal("Did not delete", i, docID)
			}
			if err = clients[i%1].valIsNotIndexed("col", []string{"a", "b"}, uint64(i*2), docID); err != nil {
				t.Fatal(err)
			}
		} else {
			if doc, err := clients[i%1].Read("col", docID); err != nil || doc["a"].(map[string]interface{})["b"].(float64) != float64(i*2) {
				t.Fatal(docID, doc)
			}
			if err = clients[i%1].valIsIndexed("col", []string{"a", "b"}, i*2, docID); err != nil {
				t.Fatal(err)
			}
		}
	}
	// Verify that there are approximately 1000 documents
	if count, err := clients[0].ApproxDocCount("col"); err != nil {
		t.Fatal(err)
	} else if count < 600 || count > 1400 {
		t.Fatal("Approximate is way off")
	}

	// If pendingUpdate counter is broken by error, server will refuse to go into maintenance mode
	if _, err = clients[0].goMaintTest(); err != nil {
		t.Fatal(err)
	} else if err = clients[0].leaveMaintTest(); err != nil {
		t.Fatal(err)
	}
	clients[0].Shutdown()
}
