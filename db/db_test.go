package db

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"path"
	"strings"
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

func TestOpenEmptyDB(t *testing.T) {
	os.RemoveAll(TEST_DATA_DIR)
	defer os.RemoveAll(TEST_DATA_DIR)
	db, err := OpenDB(TEST_DATA_DIR)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Create("a"); err != nil {
		t.Fatal(err)
	}
	if len(db.cols) != 1 {
		t.Fatal(db.cols)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestOpenCloseDB(t *testing.T) {
	os.RemoveAll(TEST_DATA_DIR)
	defer os.RemoveAll(TEST_DATA_DIR)
	if err := os.MkdirAll(TEST_DATA_DIR, 0700); err != nil {
		t.Fatal(err)
	}
	touchFile(TEST_DATA_DIR+"/ColA", "dat")
	touchFile(TEST_DATA_DIR+"/ColA", "a!b!c")
	if err := os.MkdirAll(TEST_DATA_DIR+"/ColB", 0700); err != nil {
		panic(err)
	}
	db, err := OpenDB(TEST_DATA_DIR)
	if err != nil {
		t.Fatal(err)
	}
	if db.path != TEST_DATA_DIR || db.cols["ColA"] == nil || db.cols["ColB"] == nil {
		t.Fatal(db.cols)
	}
	colA := db.cols["ColA"]
	colB := db.cols["ColB"]
	if colA.indexPaths["a!b!c"][0] != "a" || colA.indexPaths["a!b!c"][1] != "b" || colA.indexPaths["a!b!c"][2] != "c" {
		t.Fatal(colA.indexPaths)
	}
	if colA.hts["a!b!c"] == nil {
		t.Fatal(colA.hts)
	}
	if len(colB.indexPaths) != 0 || len(colB.hts) != 0 {
		t.Fatal(colB)
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
	if allNames := db.AllCols(); len(allNames) != 2 || !(allNames[0] == "a" && allNames[1] == "b") {
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
	if allNames := db.AllCols(); len(allNames) != 2 || !(allNames[0] == "c" && allNames[1] == "d") {
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
	if allNames := db.AllCols(); len(allNames) != 2 || !(allNames[0] == "c" && allNames[1] == "d") {
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
	if allNames := db.AllCols(); len(allNames) != 1 || allNames[0] != "d" {
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
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestDumpDB(t *testing.T) {
	os.RemoveAll(TEST_DATA_DIR)
	os.RemoveAll(TEST_DATA_DIR + "bak")
	defer os.RemoveAll(TEST_DATA_DIR)
	defer os.RemoveAll(TEST_DATA_DIR + "bak")
	if err := os.MkdirAll(TEST_DATA_DIR, 0700); err != nil {
		t.Fatal(err)
	}
	db, err := OpenDB(TEST_DATA_DIR)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Create("a"); err != nil {
		t.Fatal(err)
	} else if err := db.Create("b"); err != nil {
		t.Fatal(err)
	}
	err = db.Use("a").MultiShardLockDocAndInsert(123, []byte("abcde"))
	if err != nil {
		t.Fatal(err)
	} else if err := db.Dump(TEST_DATA_DIR + "bak"); err != nil {
		t.Fatal(err)
	}
	// Open the new database
	db2, err := OpenDB(TEST_DATA_DIR + "bak")
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if allCols := db2.AllCols(); !(allCols[0] == "a" && allCols[1] == "b") {
		t.Fatal(allCols)
	}
	if doc, err := db2.Use("a").BPRead(123); err != nil || strings.TrimSpace(string(doc)) != "abcde" {
		t.Fatal(string(doc), err)
	}
	if err := db2.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestStrHash(t *testing.T) {
	strings := []string{"", " ", "abc", "123"}
	hashes := []uint64{0, 32, 417419622498, 210861491250}
	for i := range strings {
		if StrHash(strings[i]) != hashes[i] {
			t.Fatalf("Hash of %s equals to %d, it should equal to %d", strings[i], StrHash(strings[i]), hashes[i])
		}
	}
}

func TestGetIn(t *testing.T) {
	var obj interface{}
	// Get inside a JSON object
	json.Unmarshal([]byte(`{"a": {"b": {"c": 1}}}`), &obj)
	if val, ok := GetIn(obj, []string{"a", "b", "c"})[0].(float64); !ok || val != 1 {
		t.Fatal()
	}
	// Get inside a JSON array
	json.Unmarshal([]byte(`{"a": {"b": {"c": [1, 2, 3]}}}`), &obj)
	if val, ok := GetIn(obj, []string{"a", "b", "c"})[0].(float64); !ok || val != 1 {
		t.Fatal()
	}
	if val, ok := GetIn(obj, []string{"a", "b", "c"})[1].(float64); !ok || val != 2 {
		t.Fatal()
	}
	if val, ok := GetIn(obj, []string{"a", "b", "c"})[2].(float64); !ok || val != 3 {
		t.Fatal()
	}
	// Get inside JSON objects contained in JSON array
	json.Unmarshal([]byte(`{"a": [{"b": {"c": [1]}}, {"b": {"c": [2, 3]}}]}`), &obj)
	if val, ok := GetIn(obj, []string{"a", "b", "c"})[0].(float64); !ok || val != 1 {
		t.Fatal()
	}
	if val, ok := GetIn(obj, []string{"a", "b", "c"})[1].(float64); !ok || val != 2 {
		t.Fatal()
	}
	if val, ok := GetIn(obj, []string{"a", "b", "c"})[2].(float64); !ok || val != 3 {
		t.Fatal()
	}
	// Get inside a JSON array and fetch attributes from array elements, which are JSON objects
	json.Unmarshal([]byte(`{"a": [{"b": {"c": [4]}}, {"b": {"c": [5, 6]}}], "d": [0, 9]}`), &obj)
	if val, ok := GetIn(obj, []string{"a", "b", "c"})[0].(float64); !ok || val != 4 {
		t.Fatal()
	}
	if val, ok := GetIn(obj, []string{"a", "b", "c"})[1].(float64); !ok || val != 5 {
		t.Fatal()
	}
	if val, ok := GetIn(obj, []string{"a", "b", "c"})[2].(float64); !ok || val != 6 {
		t.Fatal()
	}
	if len(GetIn(obj, []string{"a", "b", "c"})) != 3 {
		t.Fatal()
	}
	if val, ok := GetIn(obj, []string{"d"})[0].(float64); !ok || val != 0 {
		t.Fatal()
	}
	if val, ok := GetIn(obj, []string{"d"})[1].(float64); !ok || val != 9 {
		t.Fatal()
	}
	if len(GetIn(obj, []string{"d"})) != 2 {
		t.Fatal()
	}
	// Another example
	json.Unmarshal([]byte(`{"a": {"b": [{"c": 2}]}, "d": 0}`), &obj)
	if val, ok := GetIn(obj, []string{"a", "b", "c"})[0].(float64); !ok || val != 2 {
		t.Fatal()
	}
	if len(GetIn(obj, []string{"a", "b", "c"})) != 1 {
		t.Fatal()
	}
}
