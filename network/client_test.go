/*
Test client connection AND reach out to the server using test commands.
This test must be invoked by runtd script.
*/
package network

import (
	"fmt"
	"github.com/HouzuoGuo/tiedot/colpart"
	"github.com/HouzuoGuo/tiedot/uid"
	"math/rand"
	"strconv"
	"testing"
	"time"
)

/*
The following test conditions are coded in runtd script:
- there are 4 IPC test servers and 4 clients
- servers create socket files in /tmp/tiedot_test_ipc_tmp
- client runs 4 GOMAXPROCS
*/
const NUM_SERVERS = 4

var clients []*Client

func ClientConnect(t *testing.T) {
	clients = make([]*Client, 4)
	var err error
	for i := 0; i < 4; i++ {
		clients[i], err = NewClient("/tmp/tiedot_test_ipc_tmp", i)
		if err != nil {
			t.Fatal(err)
		}
	}
}

func Pings(t *testing.T) {
	for i := 0; i < 4; i++ {
		if !(clients[i].SrvAddr == "/tmp/tiedot_test_ipc_tmp/"+strconv.Itoa(i) && clients[i].SrvRank == i && clients[i].Conn != nil && clients[i].IPCSrvTmpDir == "/tmp/tiedot_test_ipc_tmp") {
			t.Fatal(clients[i])
		}
		if err2 := clients[i].getOK(PING); err2 != nil {
			t.Fatal(err2)
		}
		if str, err2 := clients[i].getStr(PING); str != ACK || err2 != nil {
			t.Fatal(str, err2)
		}
		if i, err2 := clients[i].getUint64(PING1); i != 1 || err2 != nil {
			t.Fatal(i, err2)
		}
		if js, err2 := clients[i].getJSON(PING_JSON); js.([]interface{})[0].(string) != "OK" || err2 != nil {
			t.Fatal(i, err2)
		}
		if _, err2 := clients[i].getStr(PING_ERR); fmt.Sprint(err2) != ERR+"this is an error" {
			t.Fatal(err2)
		}
	}
}

func ColCRUD(t *testing.T) {
	// Create collections
	if clients[0].ColCreate("z", 2) != nil {
		t.Fatal()
	}
	if clients[3].ColCreate("x", 3) != nil {
		t.Fatal()
	}
	// Get collection names
	for i := 0; i < 4; i++ {
		allCols, err := clients[i].ColAll()
		if err != nil {
			t.Fatal(err)
		}
		if !(len(allCols) == 2 && allCols["z"] == 2 && allCols["x"] == 3) {
			t.Fatal(allCols)
		}
	}
	// There are now two collections: z of 2 partitions and x of 3 partitions
	// Rename collections
	if clients[3].ColRename("z", "a") != nil { // 2 parts
		t.Fatal()
	}
	if clients[2].ColRename("x", "b") != nil { // 3 parts
		t.Fatal()
	}
	for i := 0; i < 4; i++ {
		allCols, err := clients[i].ColAll()
		if err != nil {
			t.Fatal(err)
		}
		if !(len(allCols) == 2 && allCols["a"] == 2 && allCols["b"] == 3) {
			t.Fatal(allCols)
		}
	}
	// There are now two collections: a of 2 partitions, b of 3 partitions
	// Drop a collection
	var err error

	if err = clients[3].ColDrop("b"); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 4; i++ {
		allCols, err := clients[i].ColAll()
		if err != nil {
			t.Fatal(err)
		}
		if !(len(allCols) == 1 && allCols["a"] == 2) {
			t.Fatal(allCols)
		}
	}
}

// There is now one collection: a of 2 partitions

func DocCRUD(t *testing.T) {
	var err error
	if _, err = clients[2].docInsert("a", map[string]interface{}{uid.PK_NAME: "1", "1": "1"}); err == nil {
		t.Fatal()
	}
	if _, err = clients[3].docInsert("a", map[string]interface{}{uid.PK_NAME: "2", "1": "1"}); err == nil {
		t.Fatal()
	}
	// doc insert
	docIDs := make([]uint64, 2)
	if docIDs[0], err = clients[0].docInsert("a", map[string]interface{}{uid.PK_NAME: "765", "abc": "1"}); err != nil {
		t.Fatal(err)
	}
	if docIDs[1], err = clients[1].docInsert("a", map[string]interface{}{uid.PK_NAME: "987", "abc": "2"}); err != nil {
		t.Fatal(err)
	}
	// doc read
	if _, err := clients[0].docGet("a", 12345); err == nil {
		t.Fatal()
	}
	if doc, err := clients[0].docGet("a", docIDs[0]); err != nil || doc == nil {
		t.Fatal(err)
	}
	if doc, err := clients[1].docGet("a", docIDs[1]); err != nil || doc == nil {
		t.Fatal(err)
	}
	// doc update
	if _, err = clients[0].docUpdate("a", 12345, map[string]interface{}{uid.PK_NAME: "765", "content": "a"}); err == nil {
		t.Fatal()
	}
	if docIDs[0], err = clients[0].docUpdate("a", docIDs[0], map[string]interface{}{uid.PK_NAME: "765", "content": "a"}); err != nil {
		t.Fatal(err)
	}
	if docIDs[1], err = clients[1].docUpdate("a", docIDs[1], map[string]interface{}{uid.PK_NAME: "987", "content": "b"}); err != nil {
		t.Fatal(err)
	}
	if doc, err := clients[0].docGet("a", docIDs[0]); err != nil || doc.(map[string]interface{})["content"].(string) != "a" {
		t.Fatal(err)
	}
	if doc, err := clients[1].docGet("a", docIDs[1]); err != nil || doc.(map[string]interface{})["content"].(string) != "b" {
		t.Fatal(err)
	}
	// doc delete then read
	if err = clients[1].docDelete("a", docIDs[1]); err != nil {
		t.Fatal(err)
	}
	if _, err := clients[1].docGet("a", docIDs[1]); err == nil {
		t.Fatal()
	}
	// remember, partition 0 should still have the document ID 0
	if _, err := clients[0].docGet("a", docIDs[0]); err != nil {
		t.Fatal()
	}
}

// There is now (still) one collection "a" with two partitions

func IndexCRUD(t *testing.T) {
	// Create 3 indexes
	if err := clients[0].IdxCreate("a", "a,b,c"); err != nil {
		t.Fatal(err)
	}
	if err := clients[1].IdxCreate("a", "d,e,f"); err != nil {
		t.Fatal(err)
	}
	if err := clients[2].IdxCreate("a", "g,h,i"); err != nil {
		t.Fatal(err)
	}
	// Get indexed paths
	paths, err := clients[2].IdxAll("a")
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) != 3 || paths[0] != "a,b,c" || paths[1] != "d,e,f" || paths[2] != "g,h,i" {
		t.Fatal(paths)
	}
	// Remove an index
	if err := clients[2].IdxDrop("a", "g,h,i"); err != nil {
		t.Fatal(err)
	}
	paths, err = clients[1].IdxAll("a")
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) != 2 || paths[0] != "a,b,c" || paths[1] != "d,e,f" {
		t.Fatal(paths)
	}
}

// There is now one collection "a" with two partitions, and two indexes "a,b,c", "d,e,f"

func HashCRUD(t *testing.T) {
	if err := clients[0].htPut("asdf", "asdf", 1, 1); err == nil {
		t.Fatal()
	}
	if err := clients[0].htPut("a", "asdf", 1, 1); err == nil {
		t.Fatal()
	}
	// Put some entries
	if err := clients[0].htPut("a", "a,b,c", 1, 1); err != nil {
		t.Fatal(err)
	}
	if err := clients[0].htPut("a", "a,b,c", 1, 2); err != nil {
		t.Fatal(err)
	}
	if err := clients[1].htPut("a", "a,b,c", 3, 4); err != nil {
		t.Fatal(err)
	}
	if err := clients[1].htPut("a", "a,b,c", 3, 5); err != nil {
		t.Fatal(err)
	}
	// Get key 1 and key 3
	vals1, err1 := clients[0].htGet("a", "a,b,c", 1, 0)
	if !(err1 == nil && len(vals1) == 2 && vals1[0] == 1 && vals1[1] == 2) {
		t.Fatal(vals1, err1)
	}
	vals3, err3 := clients[1].htGet("a", "a,b,c", 3, 1)
	if !(err3 == nil && len(vals3) == 1 && vals3[0] == 4) {
		t.Fatal(vals3, err3)
	}
	vals3, err3 = clients[1].htGet("a", "a,b,c", 3, 0)
	if !(err3 == nil && len(vals3) == 2 && vals3[0] == 4 && vals3[1] == 5) {
		t.Fatal(vals3, err3)
	}
	vals4, err4 := clients[1].htGet("a", "a,b,c", 4, 0)
	if !(err3 == nil && len(vals4) == 0) {
		t.Fatal(vals4, err4)
	}
	// Remove a value from key 1 and key 3
	if err := clients[0].htDelete("a", "a,b,c", 1, 1); err != nil {
		t.Fatal(err)
	}
	if err := clients[1].htDelete("a", "a,b,c", 3, 4); err != nil {
		t.Fatal(err)
	}
	vals1, err1 = clients[0].htGet("a", "a,b,c", 1, 0)
	if !(err1 == nil && len(vals1) == 1 && vals1[0] == 2) {
		t.Fatal(vals1, err1)
	}
	vals3, err3 = clients[1].htGet("a", "a,b,c", 3, 0)
	if !(err3 == nil && len(vals3) == 1 && vals3[0] == 5) {
		t.Fatal(vals3, err3)
	}
	if err := clients[1].IdxDrop("a", "a,b,c"); err != nil {
		t.Fatal(err)
	}
	if err := clients[1].IdxDrop("a", "d,e,f"); err != nil {
		t.Fatal(err)
	}
}

// There is now one collection "a" with two partitions, without any index

func DocCRUD2(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	// Insert wrong stuff
	if _, err := clients[0].ColInsert("asdf", nil); err == nil {
		t.Fatal()
	}
	if _, err := clients[1].ColInsert("a", nil); err == nil {
		t.Fatal()
	}
	var err error
	numDocs := 111
	docIDs := make([]uint64, numDocs)
	// Insert some documents
	for i := 0; i < numDocs; i++ {
		if docIDs[i], err = clients[rand.Intn(NUM_SERVERS)].ColInsert("a", map[string]interface{}{"attr": i, "extra": "abcd"}); err != nil {
			t.Fatal(err)
		}
	}
	// Read them back
	for i := 0; i < numDocs; i++ {
		doc, err := clients[rand.Intn(NUM_SERVERS)].ColGet("a", docIDs[i])
		if err != nil || doc.(map[string]interface{})["attr"].(float64) != float64(i) {
			t.Fatal(err, doc)
		}
	}
	// Update each of them
	for i := 0; i < numDocs; i++ {
		if err = clients[rand.Intn(NUM_SERVERS)].ColUpdate("a", docIDs[i], map[string]interface{}{"attr": i * 2, "extra": nil}); err != nil {
			t.Fatal(err)
		}
	}
	// Read them back - again
	for i := 0; i < numDocs; i++ {
		doc, err := clients[rand.Intn(NUM_SERVERS)].ColGet("a", docIDs[i])
		if err != nil || doc.(map[string]interface{})["attr"].(float64) != float64(i*2) ||
			doc.(map[string]interface{})[uid.PK_NAME].(string) != strconv.FormatUint(docIDs[i], 10) {
			t.Fatal(err, doc)
		}
	}
	// Delete half of them
	for i := 0; i < numDocs/2; i++ {
		if err = clients[rand.Intn(NUM_SERVERS)].ColDelete("a", docIDs[i]); err != nil {
			t.Fatal(err)
		}
	}
	// Read them back - again
	for i := 0; i < numDocs; i++ {
		doc, err := clients[rand.Intn(NUM_SERVERS)].ColGet("a", docIDs[i])
		if i < numDocs/2 && err == nil {
			// deleted half
			t.Fatal("did not delete", i)
		} else if i >= numDocs/2 && (err != nil || doc.(map[string]interface{})["attr"].(float64) != float64(i*2)) {
			// untouched half
			t.Fatal(err, doc)
		}
	}
}

func DocIndexing(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	var err error
	numDocs := 111
	numDocsPerIter := 7 // do not change
	numParts := 3
	docIDs := make([]uint64, numDocs*numDocsPerIter)
	if err = clients[0].ColCreate("index", numParts); err != nil {
		t.Fatal(err)
	}
	// Prepare numDocs * numDocsPerIter documents
	clients[0].IdxCreate("index", "a,b")
	for i := 0; i < numDocs; i++ {
		docs := []map[string]interface{}{
			map[string]interface{}{"a": map[string]interface{}{"b": (0 * numDocs) + (i + 1)}, "extra": "abc"},
			map[string]interface{}{"a": []interface{}{map[string]interface{}{"b": (1 * numDocs) + (i + 1), "extra": "abc"}, "bcd"}, "extra": "cde"},
			map[string]interface{}{"a": []interface{}{map[string]interface{}{"b": []interface{}{(2 * numDocs) + (i + 1)}, "extra": "abc"}, "bcd"}, "extra": "cde"},
			map[string]interface{}{"a": nil, "extra": "abc"},
			map[string]interface{}{"a": map[string]interface{}{"b": nil, "extra": "bcd"}},
			map[string]interface{}{"a": []interface{}{map[string]interface{}{"b": nil, "extra": "abc"}, "bcd"}, "extra": "cde"},
			map[string]interface{}{"a": []interface{}{map[string]interface{}{"b": []interface{}{nil}, "extra": "abc"}, "bcd"}, "extra": "cde"}}
		for j, doc := range docs {
			if docIDs[i*numDocsPerIter+j], err = clients[rand.Intn(NUM_SERVERS)].ColInsert("index", doc); err != nil {
				t.Fatal(err)
			}
		}
	}
	// Test every indexed entry
	for i := 0; i < numDocs; i++ {
		for j := 0; j < 3; j++ {
			// Figure out where the index value went
			theDocID := docIDs[i*numDocsPerIter+j]
			hashKey := colpart.StrHash(fmt.Sprint((j * numDocs) + (i + 1)))
			partNum := int(hashKey % uint64(numParts))
			// Fetch index value by key
			vals, err := clients[partNum].htGet("index", "a,b", hashKey, 0)
			if err != nil {
				t.Fatal(err)
			}
			if !(len(vals) == 1 && vals[0] == theDocID) {
				t.Fatal(i, j, theDocID, hashKey, partNum, vals)
			}
		}
	}
	// Update every indexed entry
	for i := 0; i < numDocs; i++ {
		docs := []map[string]interface{}{
			map[string]interface{}{"a": map[string]interface{}{"b": (0 * numDocs) + (i + 2)}, "extra": "abc"},
			map[string]interface{}{"a": []interface{}{map[string]interface{}{"b": (1 * numDocs) + (i + 2), "extra": "abc"}, "bcd"}, "extra": "cde"},
			map[string]interface{}{"a": []interface{}{map[string]interface{}{"b": []interface{}{(2 * numDocs) + (i + 2)}, "extra": "abc"}, "bcd"}, "extra": "cde"},
			map[string]interface{}{"a": nil, "extra": "abc"},
			map[string]interface{}{"a": map[string]interface{}{"b": nil, "extra": "bcd"}},
			map[string]interface{}{"a": []interface{}{map[string]interface{}{"b": nil, "extra": "abc"}, "bcd"}, "extra": "cde"},
			map[string]interface{}{"a": []interface{}{map[string]interface{}{"b": []interface{}{nil}, "extra": "abc"}, "bcd"}, "extra": "cde"}}
		for j, doc := range docs {
			if err = clients[rand.Intn(NUM_SERVERS)].ColUpdate("index", docIDs[i*numDocsPerIter+j], doc); err != nil {
				t.Fatal(err)
			}
		}
	}
	// Test every indexed entry
	for i := 0; i < numDocs; i++ {
		// Test new values
		for j := 0; j < 3; j++ {
			// Figure out where the index value went
			theDocID := docIDs[i*numDocsPerIter+j]
			hashKey := colpart.StrHash(fmt.Sprint((j * numDocs) + (i + 2)))
			partNum := int(hashKey % uint64(numParts))
			// Fetch index value by key
			vals, err := clients[partNum].htGet("index", "a,b", hashKey, 0)
			if err != nil {
				t.Fatal(err)
			}
			if !(len(vals) == 1 && vals[0] == theDocID) {
				t.Fatal(i, j, theDocID, hashKey, partNum, vals)
			}
		}
		// Old values are gone
		for j := 0; j < 3; j++ {
			// Figure out where the index value went
			theDocID := docIDs[i*numDocsPerIter+j]
			hashKey := colpart.StrHash(fmt.Sprint((j * numDocs) + (i + 1)))
			partNum := int(hashKey % uint64(numParts))
			// Fetch index value by key
			vals, err := clients[partNum].htGet("index", "a,b", hashKey, 0)
			if err != nil {
				t.Fatal(err)
			}
			for _, val := range vals {
				if val == theDocID {
					t.Fatal(i, j, theDocID, hashKey, partNum, vals)
				}
			}
		}
	}
	// Delete every indexed entry
	for _, id := range docIDs {
		if err = clients[rand.Intn(NUM_SERVERS)].ColDelete("index", id); err != nil {
			t.Fatal(err)
		}
	}
	// Test every indexed entry
	for i := 0; i < numDocs; i++ {
		// Test new values
		for j := 0; j < 3; j++ {
			// Figure out where the index value went
			theDocID := docIDs[i*numDocsPerIter+j]
			hashKey := colpart.StrHash(fmt.Sprint((j * numDocs) + (i + 2)))
			partNum := int(hashKey % uint64(numParts))
			// Fetch index value by key
			vals, err := clients[partNum].htGet("index", "a,b", hashKey, 0)
			if err != nil {
				t.Fatal(err)
			}
			if len(vals) > 0 {
				t.Fatal(i, j, theDocID, hashKey, vals)
			}
		}
	}
}

func ServerShutdown(t *testing.T) {
	for i := 0; i < 4; i++ {
		clients[i].ShutdownServer()
	}
}

// Main test entrance - to ensure that all tests are executed in correct order.
func TestSequence(t *testing.T) {
	ClientConnect(t)
	Pings(t)
	ColCRUD(t)
	DocCRUD(t)
	IndexCRUD(t)
	HashCRUD(t)
	DocCRUD2(t)
	DocIndexing(t)
	ServerShutdown(t)
}
