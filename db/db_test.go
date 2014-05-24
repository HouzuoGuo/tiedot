package db

import (
	"io/ioutil"
	"os"
	"path"
	"testing"
)

const (
	TEST_DATA_DIR = "/tmp/tiedot_test"
)

func touchFile(dir, filename string) {
	if err := os.MkdirAll(dir, 0700); err != nil {
		panic(err)
	}
	if err := ioutil.WriteFile(path.Join(dir, filename), make([]byte, 0), 0600); err != nil {
		panic(err)
	}
}

func TestOpenSyncCloseDB(t *testing.T) {
	os.RemoveAll(TEST_DATA_DIR)
	defer os.RemoveAll(TEST_DATA_DIR)
	if err := os.MkdirAll(TEST_DATA_DIR, 0700); err != nil {
		t.Fatal(err)
	}
	if err := ioutil.WriteFile(TEST_DATA_DIR+"/number_of_partitions", []byte("2"), 0600); err != nil {
		t.Fatal(err)
	}
	touchFile(TEST_DATA_DIR+"/ColA", "dat_0")
	touchFile(TEST_DATA_DIR+"/ColA/a!b!c", "0")
	if err := os.MkdirAll(TEST_DATA_DIR+"/ColB", 0700); err != nil {
		panic(err)
	}

	db, err := OpenDB(TEST_DATA_DIR)
	if err != nil {
		t.Fatal(err)
	}
	if db.path != TEST_DATA_DIR || db.numParts != 2 || db.cols["ColA"] == nil || db.cols["ColB"] == nil {
		t.Fatal(db.cols)
	}
	colA := db.cols["ColA"]
	colB := db.cols["ColB"]
	if len(colA.parts) != 2 || len(colA.hts) != 2 {
		t.Fatal(colA)
	}
	if colA.indexPaths["a!b!c"][0] != "a" || colA.indexPaths["a!b!c"][1] != "b" || colA.indexPaths["a!b!c"][2] != "c" {
		t.Fatal(colA.indexPaths)
	}
	if colA.hts[0]["a!b!c"] == nil || colA.hts[1]["a!b!c"] == nil {
		t.Fatal(colA.hts)
	}
	if len(colB.parts) != 2 || len(colB.hts) != 2 {
		t.Fatal(colB)
	}
	if err := db.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestColCrud(t *testing.T) {
	os.RemoveAll(TEST_DATA_DIR)
	defer os.RemoveAll(TEST_DATA_DIR)
	if err := os.MkdirAll(TEST_DATA_DIR, 0700); err != nil {
		t.Fatal(err)
	}
	if err := ioutil.WriteFile(TEST_DATA_DIR+"/number_of_partitions", []byte("2"), 0600); err != nil {
		t.Fatal(err)
	}
	db, err := OpenDB(TEST_DATA_DIR)
	if err != nil {
		t.Fatal(err)
	}
	if len(db.AllCols()) != 0 {
		t.Fatal(db.AllCols())
	}
	// Create
	if err := db.Create("a"); err != nil {
		t.Fatal(err)
	}
	if db.Create("a") == nil {
		t.Fatal("Did not error")
	}
	if err := db.Create("b"); err != nil {
		t.Fatal(err)
	}
	// Get all names & use
	if allNames := db.AllCols(); !(allNames[0] == "a" && allNames[1] == "b") {
		t.Fatal(allNames)
	}
	if db.Use("a") == nil || db.Use("b") == nil || db.Use("abcde") != nil {
		t.Fatal(db.cols)
	}
	// Rename
	if db.Rename("a", "a") == nil {
		t.Fatal("Did not error")
	}
	if db.Rename("a", "b") == nil {
		t.Fatal("Did not error")
	}
	if db.Rename("abc", "b") == nil {
		t.Fatal("Did not error")
	}
	if err := db.Rename("a", "c"); err != nil {
		t.Fatal(err)
	}
	if err := db.Rename("b", "d"); err != nil {
		t.Fatal(err)
	}
	// Rename - verify
	if allNames := db.AllCols(); !(allNames[0] == "d" && allNames[1] == "c") {
		t.Fatal(allNames)
	}
	if db.Use("c") == nil || db.Use("d") == nil || db.Use("a") != nil {
		t.Fatal(db.cols)
	}
	// Truncate
	if db.Truncate("a") == nil {
		t.Fatal("Did not error")
	}
	if err := db.Truncate("c"); err != nil {
		t.Fatal(err)
	}
	if err := db.Truncate("d"); err != nil {
		t.Fatal(err)
	}
	// Truncate - verify
	if allNames := db.AllCols(); !(allNames[0] == "d" && allNames[1] == "c") {
		t.Fatal(allNames)
	}
	if db.Use("c") == nil || db.Use("d") == nil || db.Use("a") != nil {
		t.Fatal(db.cols)
	}
	// Drop
	if db.Drop("a") == nil {
		t.Fatal("Did not error")
	}
	if err := db.Drop("c"); err != nil {
		t.Fatal(err)
	}
	if allNames := db.AllCols(); allNames[0] != "d" {
		t.Fatal(allNames)
	}
	if db.Use("d") == nil {
		t.Fatal(db.cols)
	}
	if err := db.Drop("d"); err != nil {
		t.Fatal(err)
	}
	if allNames := db.AllCols(); len(allNames) != 0 {
		t.Fatal(allNames)
	}
	if db.Use("d") != nil {
		t.Fatal(db.cols)
	}
	// Sync & close
	if err := db.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}
