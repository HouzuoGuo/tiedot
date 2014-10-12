package binprot

import (
	"os"
	"testing"
)

func TestColCrud(t *testing.T) {
	os.RemoveAll(WS)
	defer os.RemoveAll(WS)
	_, clients := mkServersClients(2)
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
	// Rename - verify
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
	clients[0].Shutdown()
}

func TestDumpDB(t *testing.T) {
	bak_dir := WS + "_bak"
	os.RemoveAll(WS)
	os.RemoveAll(bak_dir)
	defer os.RemoveAll(WS)
	defer os.RemoveAll(bak_dir)
	_, clients := mkServersClients(2)
	if err := clients[0].Create("a"); err != nil {
		t.Fatal(err)
	} else if err = clients[0].Index("a", []string{"1"}); err != nil {
		t.Fatal(err)
	} else if err := clients[1].Create("b"); err != nil {
		t.Fatal(err)
	} else if err = clients[1].Index("b", []string{"2"}); err != nil {
		t.Fatal(err)
	} else if err = clients[0].DumpDB(bak_dir); err != nil {
		t.Fatal(err)
	}
	clients[1].Shutdown()
	if err := os.RemoveAll(WS); err != nil {
		t.Fatal(err)
	} else if err = os.Rename(bak_dir, WS); err != nil {
		t.Fatal(err)
	}
	_, clients = mkServersClients(2)
	if clients[0].AllCols()[0] != "a" || clients[1].AllCols()[1] != "b" {
		t.Fatal(clients[0].AllCols())
	} else if len(clients[0].indexPaths) != 2 || len(clients[1].indexPaths[0]) != 1 {
		t.Fatal(clients[0].indexPaths, clients[1].indexPaths)
	}
	clients[0].Shutdown()
}

func TestIdxCrud(t *testing.T) {
	os.RemoveAll(WS)
	defer os.RemoveAll(WS)
	_, clients := mkServersClients(2)

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
}
