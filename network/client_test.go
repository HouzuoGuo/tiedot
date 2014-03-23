/*
Test client connection AND reach out to the server using test commands.
This test must be invoked by runtd script.
*/
package network

import (
	"fmt"
	"github.com/HouzuoGuo/tiedot/colpart"
	"github.com/HouzuoGuo/tiedot/uid"
	"strconv"
	"testing"
)

/*
The following test conditions are coded in runtd script:
- there are 4 IPC test servers
- servers create socket files in /tmp/tiedot_test_ipc_tmp
- client runs 4 GOMAXPROCS
*/
const NUM_SERVERS = 4

var client *Client

func ClientConnect(t *testing.T) {
	var err error
	client, err = NewClient(NUM_SERVERS, "/tmp/tiedot_test_ipc_tmp")
	if err != nil {
		t.Fatal(err)
	}
}

func Pings(t *testing.T) {
	if !(client.TotalRank == NUM_SERVERS && client.IPCSrvTmpDir == "/tmp/tiedot_test_ipc_tmp") {
		t.Fatal(client)
	}
	for i := 0; i < 4; i++ {
		if err2 := client.getOK(i, PING); err2 != nil {
			t.Fatal(err2)
		}
		if str, err2 := client.getStr(i, PING); str != ACK || err2 != nil {
			t.Fatal(str, err2)
		}
		if i, err2 := client.getUint64(i, PING1); i != 1 || err2 != nil {
			t.Fatal(i, err2)
		}
		if js, err2 := client.getJSON(i, PING_JSON); js.([]interface{})[0].(string) != "OK" || err2 != nil {
			t.Fatal(i, err2)
		}
		if _, err2 := client.getStr(i, PING_ERR); fmt.Sprint(err2) != ERR+"this is an error" {
			t.Fatal(err2)
		}
	}
}

func ColCRUD(t *testing.T) {
	// Create collections
	if client.ColCreate("z") != nil {
		t.Fatal()
	}
	if client.ColCreate("x") != nil {
		t.Fatal()
	}
	// Get collection names
	allCols, err := client.ColAll()
	if err != nil {
		t.Fatal(err)
	}
	if !(len(allCols) == 2 && allCols["z"] == 4 && allCols["x"] == 4) {
		t.Fatal(allCols)
	}
	// Rename collections
	if client.ColRename("z", "a") != nil { // 2 parts
		t.Fatal()
	}
	if client.ColRename("x", "b") != nil { // 3 parts
		t.Fatal()
	}
	allCols, err = client.ColAll()
	if err != nil {
		t.Fatal(err)
	}
	if !(len(allCols) == 2 && allCols["a"] == 4 && allCols["b"] == 4) {
		t.Fatal(allCols)
	}
	// Drop a collection
	if err = client.ColDrop("b"); err != nil {
		t.Fatal(err)
	}
	allCols, err = client.ColAll()
	if err != nil {
		t.Fatal(err)
	}
	if !(len(allCols) == 1 && allCols["a"] == 4) {
		t.Fatal(allCols)
	}
}

// There is now one collection "a"

func IndexCRUD(t *testing.T) {
	// Create 3 indexes
	if err := client.IdxCreate("a", "a,b,c"); err != nil {
		t.Fatal(err)
	}
	if err := client.IdxCreate("a", "d,e,f"); err != nil {
		t.Fatal(err)
	}
	if err := client.IdxCreate("a", "g,h,i"); err != nil {
		t.Fatal(err)
	}
	// Get indexed paths
	paths, err := client.IdxAll("a")
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) != 3 || paths[0] != "a,b,c" || paths[1] != "d,e,f" || paths[2] != "g,h,i" {
		t.Fatal(paths)
	}
	// Remove an index
	if err := client.IdxDrop("a", "g,h,i"); err != nil {
		t.Fatal(err)
	}
	paths, err = client.IdxAll("a")
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) != 2 || paths[0] != "a,b,c" || paths[1] != "d,e,f" {
		t.Fatal(paths)
	}
}

// There is now one collection "a" with two partitions, and two indexes "a,b,c", "d,e,f"

func HashCRUD(t *testing.T) {
	if err := client.htPut("asdf", "asdf", 1, 1); err == nil {
		t.Fatal()
	}
	if err := client.htPut("a", "asdf", 1, 1); err == nil {
		t.Fatal()
	}
	// Put some entries
	if err := client.htPut("a", "a,b,c", 1, 1); err != nil {
		t.Fatal(err)
	}
	if err := client.htPut("a", "a,b,c", 1, 2); err != nil {
		t.Fatal(err)
	}
	if err := client.htPut("a", "a,b,c", 3, 4); err != nil {
		t.Fatal(err)
	}
	if err := client.htPut("a", "a,b,c", 3, 5); err != nil {
		t.Fatal(err)
	}
	// Get key 1 and key 3
	vals1, err1 := client.htGet("a", "a,b,c", 1, 0)
	if !(err1 == nil && len(vals1) == 2 && vals1[0] == 1 && vals1[1] == 2) {
		t.Fatal(vals1, err1)
	}
	vals3, err3 := client.htGet("a", "a,b,c", 3, 1)
	if !(err3 == nil && len(vals3) == 1 && vals3[0] == 4) {
		t.Fatal(vals3, err3)
	}
	vals3, err3 = client.htGet("a", "a,b,c", 3, 0)
	if !(err3 == nil && len(vals3) == 2 && vals3[0] == 4 && vals3[1] == 5) {
		t.Fatal(vals3, err3)
	}
	vals4, err4 := client.htGet("a", "a,b,c", 4, 0)
	if !(err3 == nil && len(vals4) == 0) {
		t.Fatal(vals4, err4)
	}
	// Remove a value from key 1 and key 3
	if err := client.htDelete("a", "a,b,c", 1, 1); err != nil {
		t.Fatal(err)
	}
	if err := client.htDelete("a", "a,b,c", 3, 4); err != nil {
		t.Fatal(err)
	}
	vals1, err1 = client.htGet("a", "a,b,c", 1, 0)
	if !(err1 == nil && len(vals1) == 1 && vals1[0] == 2) {
		t.Fatal(vals1, err1)
	}
	vals3, err3 = client.htGet("a", "a,b,c", 3, 0)
	if !(err3 == nil && len(vals3) == 1 && vals3[0] == 5) {
		t.Fatal(vals3, err3)
	}
	if err := client.IdxDrop("a", "a,b,c"); err != nil {
		t.Fatal(err)
	}
	if err := client.IdxDrop("a", "d,e,f"); err != nil {
		t.Fatal(err)
	}
}

// There is now one collection "a" with two partitions, without any index

func DocCRUD2(t *testing.T) {
	// Insert wrong stuff
	if _, err := client.ColInsert("asdf", map[string]interface{}{}); err == nil {
		t.Fatal()
	}
	if _, err := client.ColInsert("a", nil); err == nil {
		t.Fatal()
	}
	var err error
	numDocs := 111
	docIDs := make([]uint64, numDocs)
	// Insert some documents
	for i := 0; i < numDocs; i++ {
		if docIDs[i], err = client.ColInsert("a", map[string]interface{}{"attr": i, "extr a": " abcd "}); err != nil {
			t.Fatal(err)
		}
	}
	// Read them back
	for i := 0; i < numDocs; i++ {
		doc, err := client.ColGet("a", docIDs[i])
		if err != nil || doc.(map[string]interface{})["attr"].(float64) != float64(i) {
			t.Fatal(err, doc)
		}
	}
	// Update each of them
	for i := 0; i < numDocs; i++ {
		if err = client.ColUpdate("a", docIDs[i], map[string]interface{}{"attr": i * 2, " !extra ": nil}); err != nil {
			t.Fatal(err)
		}
	}
	// Read them back - again
	for i := 0; i < numDocs; i++ {
		doc, err := client.ColGet("a", docIDs[i])
		if err != nil || doc.(map[string]interface{})["attr"].(float64) != float64(i*2) ||
			doc.(map[string]interface{})[uid.PK_NAME].(string) != strconv.FormatUint(docIDs[i], 10) {
			t.Fatal(err, doc)
		}
	}
	// Delete half of them
	for i := 0; i < numDocs/2; i++ {
		if err = client.ColDelete("a", docIDs[i]); err != nil {
			t.Fatal(err)
		}
	}
	// Read them back - again
	for i := 0; i < numDocs; i++ {
		doc, err := client.ColGet("a", docIDs[i])
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
	var err error
	numDocs := 111
	numDocsPerIter := 7 // do not change
	docIDs := make([]uint64, numDocs*numDocsPerIter)
	if err = client.ColCreate("index"); err != nil {
		t.Fatal(err)
	}
	// Prepare numDocs * numDocsPerIter documents
	client.IdxCreate("index", "a,b")
	for i := 0; i < numDocs; i++ {
		docs := []map[string]interface{}{
			map[string]interface{}{"a": map[string]interface{}{"b": (0 * numDocs) + (i + 1)}, "ext ra": "ab '!@#c"},
			map[string]interface{}{"a": []interface{}{map[string]interface{}{"b": (1 * numDocs) + (i + 1), "ext ra": "ab '!@#c"}, "bcd"}, "extra": "cde"},
			map[string]interface{}{"a": []interface{}{map[string]interface{}{"b": []interface{}{(2 * numDocs) + (i + 1)}, "ex tra": "ab '!@#c"}, "bcd"}, "extra ": "ab '!@#c"},
			map[string]interface{}{"a": nil, "extr !a": "!abc"},
			map[string]interface{}{"a": map[string]interface{}{"b": nil, "e xtra": "bc d "}},
			map[string]interface{}{"a": []interface{}{map[string]interface{}{"b": nil, "extra": "abc"}, "bcd"}, " extra": "ab '!@#c"},
			map[string]interface{}{"a": []interface{}{map[string]interface{}{"b": []interface{}{nil}, "e xtra": "ab '!@#c"}, "bcd"}, "extr a": "ab '!@#c"}}
		for j, doc := range docs {
			if docIDs[i*numDocsPerIter+j], err = client.ColInsert("index", doc); err != nil {
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
			// Fetch index value by key
			vals, err := client.htGet("index", "a,b", hashKey, 0)
			if err != nil {
				t.Fatal(err)
			}
			if !(len(vals) == 1 && vals[0] == theDocID) {
				t.Fatal(i, j, theDocID, hashKey, vals)
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
			if err = client.ColUpdate("index", docIDs[i*numDocsPerIter+j], doc); err != nil {
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
			// Fetch index value by key
			vals, err := client.htGet("index", "a,b", hashKey, 0)
			if err != nil {
				t.Fatal(err)
			}
			if !(len(vals) == 1 && vals[0] == theDocID) {
				t.Fatal(i, j, theDocID, hashKey, vals)
			}
		}
		// Old values are gone
		for j := 0; j < 3; j++ {
			// Figure out where the index value went
			theDocID := docIDs[i*numDocsPerIter+j]
			hashKey := colpart.StrHash(fmt.Sprint((j * numDocs) + (i + 1)))
			// Fetch index value by key
			vals, err := client.htGet("index", "a,b", hashKey, 0)
			if err != nil {
				t.Fatal(err)
			}
			for _, val := range vals {
				if val == theDocID {
					t.Fatal(i, j, theDocID, hashKey, vals)
				}
			}
		}
	}
	// Delete every indexed entry
	for _, id := range docIDs {
		if err = client.ColDelete("index", id); err != nil {
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
			// Fetch index value by key
			vals, err := client.htGet("index", "a,b", hashKey, 0)
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
		client.ShutdownServer()
	}
}

// Main test entrance - to ensure that all tests are executed in correct order.
func TestSequence(t *testing.T) {
	ClientConnect(t)
	Pings(t)
	ColCRUD(t)
	IndexCRUD(t)
	HashCRUD(t)
	DocCRUD2(t)
	DocIndexing(t)
	ServerShutdown(t)
}
