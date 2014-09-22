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
	}
	if clients[1].Create("a") == nil {
		t.Fatal("Did not error")
	}
	if err := clients[1].Create("b"); err != nil {
		t.Fatal(err)
	}
	// Get all names
	if allNames := clients[1].AllCols(); len(allNames) != 2 || !(allNames[0] == "a" && allNames[1] == "b") {
		t.Fatal(allNames)
	}
	// Rename
	if clients[0].Rename("a", "a") == nil {
		t.Fatal("Did not error")
	}
	if clients[0].Rename("a", "b") == nil {
		t.Fatal("Did not error")
	}
	if clients[0].Rename("abc", "b") == nil {
		t.Fatal("Did not error")
	}
	if err := clients[1].Rename("a", "c"); err != nil {
		t.Fatal(err)
	}
	if err := clients[1].Rename("b", "d"); err != nil {
		t.Fatal(err)
	}
	// Rename - verify
	if allNames := clients[1].AllCols(); len(allNames) != 2 || !(allNames[0] == "c" && allNames[1] == "d") {
		t.Fatal(allNames)
	}
	// Truncate
	if clients[0].Truncate("a") == nil {
		t.Fatal("Did not error")
	}
	if err := clients[0].Truncate("c"); err != nil {
		t.Fatal(err)
	}
	if err := clients[0].Truncate("d"); err != nil {
		t.Fatal(err)
	}
	// Truncate - verify
	if allNames := clients[1].AllCols(); len(allNames) != 2 || !(allNames[0] == "c" && allNames[1] == "d") {
		t.Fatal(allNames)
	}
	// Drop
	if clients[1].Drop("a") == nil {
		t.Fatal("Did not error")
	}
	if err := clients[1].Drop("c"); err != nil {
		t.Fatal(err)
	}
	if allNames := clients[0].AllCols(); len(allNames) != 1 || allNames[0] != "d" {
		t.Fatal(allNames)
	}
	if err := clients[0].Drop("d"); err != nil {
		t.Fatal(err)
	}
	if allNames := clients[0].AllCols(); len(allNames) != 0 {
		t.Fatal(allNames)
	}
	clients[0].Shutdown()
}
