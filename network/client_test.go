/*
Test client connection AND reach out to the server using test commands.
This test must be invoked by runtd script.
*/
package network

import (
	"fmt"
	"github.com/HouzuoGuo/tiedot/uid"
	"strconv"
	"testing"
)

/*
 Many of the following conditions are coded in runtd script:
 - there are 4 IPC test servers and 4 clients
 - servers create socket files in /tmp/tiedot_test_ipc_tmp
*/
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

func ColCreate(t *testing.T) {
	if clients[0].ColCreate("z", 2) != nil {
		t.Fatal()
	}
	if clients[3].ColCreate("x", 3) != nil {
		t.Fatal()
	}
	for i := 0; i < 4; i++ {
		allCols, err := clients[i].ColAll()
		if err != nil {
			t.Fatal(err)
		}
		if !(len(allCols) == 2 && allCols["z"] == 2 && allCols["x"] == 3) {
			t.Fatal(allCols)
		}
	}
}

// There are now two collections: z of 2 partitions and x of 3 partitions

func ColRename(t *testing.T) {
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
}

// There are now two collections: a of 2 partitions, b of 3 partitions

func ColDrop(t *testing.T) {
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
	if _, err = clients[2].docInsert("a", map[string]interface{}{"1": "1"}); err == nil {
		t.Fatal()
	}
	if _, err = clients[3].docInsert("a", map[string]interface{}{"1": "1"}); err == nil {
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
	if _, err = clients[0].docUpdate("a", 12345, map[string]interface{}{"content": "a"}); err == nil {
		t.Fatal()
	}
	if docIDs[0], err = clients[0].docUpdate("a", docIDs[0], map[string]interface{}{"content": "a"}); err != nil {
		t.Fatal(err)
	}
	if docIDs[1], err = clients[1].docUpdate("a", docIDs[1], map[string]interface{}{"content": "b"}); err != nil {
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

func HashCRUD(t *testing.T) {
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
	ColCreate(t)
	ColRename(t)
	ColDrop(t)
	DocCRUD(t)
	HashCRUD(t)
	ServerShutdown(t)
}
