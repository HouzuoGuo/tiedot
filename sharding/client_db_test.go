package sharding

import (
	"os"
	"testing"
)

func TestColCrud(t *testing.T) {
	ws, _, clients := mkServersClients(2)
	defer os.RemoveAll(ws)
	if len(clients[0].AllCols()) != 0 {
		t.Fatal(clients[0].AllCols())
	}
	// Create
	if err := clients[0].Create("a"); err != nil {
		t.Fatal(err)
	} else if clients[1].Create("a") == nil {
		t.Fatal("Did not error")
	} else if err := clients[1].Create("b"); err != nil {
		t.Fatal(err)
	}
	// Get all names
	if allNames := clients[1].AllCols(); len(allNames) != 2 || !(allNames[0] == "a" && allNames[1] == "b") {
		t.Fatal(allNames)
	}
	// Rename
	if clients[0].Rename("a", "a") == nil {
		t.Fatal("Did not error")
	} else if clients[0].Rename("a", "b") == nil {
		t.Fatal("Did not error")
	} else if clients[0].Rename("abc", "b") == nil {
		t.Fatal("Did not error")
	} else if err := clients[1].Rename("a", "c"); err != nil {
		t.Fatal(err)
	} else if err := clients[1].Rename("b", "d"); err != nil {
		t.Fatal(err)
	}
	// Rename - verify (the remaining collections are c and d)
	if allNames := clients[1].AllCols(); len(allNames) != 2 || !(allNames[0] == "c" && allNames[1] == "d") {
		t.Fatal(allNames)
	}
	// Truncate
	if clients[0].Truncate("a") == nil {
		t.Fatal("Did not error")
	} else if err := clients[0].Truncate("c"); err != nil {
		t.Fatal(err)
	} else if err := clients[0].Truncate("d"); err != nil {
		t.Fatal(err)
	}
	// Truncate - verify
	if allNames := clients[1].AllCols(); len(allNames) != 2 || !(allNames[0] == "c" && allNames[1] == "d") {
		t.Fatal(allNames)
	}
	// Scrub
	if clients[1].Scrub("a") == nil {
		t.Fatal("Did not error")
	} else if err := clients[0].Scrub("c"); err != nil {
		t.Fatal(err)
	} else if err := clients[1].Scrub("d"); err != nil {
		t.Fatal(err)
	}
	// Scrub - verify (more scrub tests are in client_doc_test.go)
	if allNames := clients[1].AllCols(); len(allNames) != 2 || !(allNames[0] == "c" && allNames[1] == "d") {
		t.Fatal(allNames)
	}
	// Drop
	if clients[1].Drop("a") == nil {
		t.Fatal("Did not error")
	} else if err := clients[1].Drop("c"); err != nil {
		t.Fatal(err)
	} else if allNames := clients[0].AllCols(); len(allNames) != 1 || allNames[0] != "d" {
		t.Fatal(allNames)
	} else if err := clients[0].Drop("d"); err != nil {
		t.Fatal(err)
	} else if allNames := clients[0].AllCols(); len(allNames) != 0 {
		t.Fatal(allNames)
	}
	clients[1].Shutdown()
	clients[0].Shutdown()
}

func TestDumpDB(t *testing.T) {
	ws, _, clients := mkServersClients(2)
	bak_dir := ws + "_bak"
	os.RemoveAll(bak_dir)
	defer os.RemoveAll(ws)
	defer os.RemoveAll(bak_dir)
	if err := clients[0].Create("a"); err != nil {
		t.Fatal(err)
	} else if err = clients[0].Index("a", []string{"1"}); err != nil {
		t.Fatal(err)
	} else if err := clients[1].Create("b"); err != nil {
		t.Fatal(err)
	} else if err = clients[1].Index("b", []string{"2"}); err != nil {
		t.Fatal(err)
	} else if err = clients[0].Backup(bak_dir); err != nil {
		t.Fatal(err)
	}
	clients[1].Shutdown()
	if err := os.RemoveAll(ws); err != nil {
		t.Fatal(err)
	} else if err = os.Rename(bak_dir, ws); err != nil {
		t.Fatal(err)
	}
	_, clients = mkServersClientsReuseWS(ws, 2)
	if clients[0].AllCols()[0] != "a" || clients[1].AllCols()[1] != "b" {
		t.Fatal(clients[0].AllCols())
	}
	// Test backup collection A
	if colAID, exists := clients[0].dbo.GetColIDByName("a"); !exists {
		t.Fatal(clients[0].dbo)
	} else if indexes := clients[0].dbo.GetIndexesByColID(colAID); len(indexes) != 1 {
		t.Fatal(indexes)
	} else if idxID := clients[0].dbo.GetIndexIDBySplitPath(colAID, []string{"1"}); idxID == -1 {
		t.Fatal(clients[0].dbo)
	} else if indexes[idxID][0] != "1" {
		t.Fatal(indexes)
	}
	// Test backup collection B
	if colBID, exists := clients[0].dbo.GetColIDByName("b"); !exists {
		t.Fatal(clients[0].dbo)
	} else if indexes := clients[0].dbo.GetIndexesByColID(colBID); len(indexes) != 1 {
		t.Fatal(indexes)
	} else if idxID := clients[0].dbo.GetIndexIDBySplitPath(colBID, []string{"2"}); idxID == -1 {
		t.Fatal(clients[0].dbo)
	} else if indexes[idxID][0] != "2" {
		t.Fatal(indexes)
	}
	clients[0].Shutdown()
	clients[1].Shutdown()
}

func TestIdxCrud(t *testing.T) {
	ws, _, clients := mkServersClients(2)
	defer os.RemoveAll(ws)

	if err := clients[0].Create("col"); err != nil {
		t.Fatal(err)
	} else if _, err := clients[0].AllIndexes("doesnotexist"); err == nil {
		t.Fatal("should error")
	} else if indexes, err := clients[1].AllIndexes("col"); err != nil || len(indexes) != 0 {
		t.Fatal(indexes, err)
		// Create new indexe
	} else if err = clients[0].Index("col", []string{"a", "b"}); err != nil {
		t.Fatal(err)
	} else if err = clients[1].Index("jiowefjiwae", []string{"a"}); err == nil {
		t.Fatal("did not error")
	} else if err = clients[0].Index("col", []string{"a", "b"}); err == nil {
		t.Fatal("did not error")
	} else if indexes, err := clients[1].AllIndexes("col"); err != nil || len(indexes) != 1 || indexes[0][0] != "a" || indexes[0][1] != "b" {
		t.Fatal(indexes, err)
		// Create more indexes
	} else if err = clients[0].Index("col", []string{"c"}); err != nil {
		t.Fatal(err)
	} else if indexes, err := clients[1].AllIndexesJointPaths("col"); err != nil || indexes[0] != "a!b" || indexes[1] != "c" {
		t.Fatal(indexes, err)
		// Unindex
	} else if clients[0].Unindex("col", []string{"%&^*"}) == nil {
		t.Fatal("Did not error")
	} else if err = clients[1].Unindex("col", []string{"c"}); err != nil {
		t.Fatal(err)
	} else if indexes, err := clients[0].AllIndexes("col"); err != nil || len(indexes) != 1 || indexes[0][0] != "a" || indexes[0][1] != "b" {
		t.Fatal(indexes, err)
	} else if err = clients[1].Unindex("col", []string{"a", "b"}); err != nil {
		t.Fatal(err)
	} else if indexes, err := clients[0].AllIndexes("col"); err != nil || len(indexes) != 0 {
		t.Fatal(indexes, err)
	}
	clients[1].Shutdown()
	clients[0].Shutdown()
}
