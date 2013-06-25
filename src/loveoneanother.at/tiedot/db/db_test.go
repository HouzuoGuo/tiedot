package db

import (
	"encoding/json"
	"os"
	"testing"
)

func TestCRUD(t *testing.T) {
	tmp := "/tmp/tiedot_db_test"
	os.RemoveAll(tmp)
	defer os.RemoveAll(tmp)
	db, err := OpenDB(tmp)
	if err != nil {
		t.Fatalf("Failed to open: %v", err)
		return
	}
	defer db.Close()
	// create
	if err := db.Create("a"); err != nil {
		t.Fatalf("Failed to create: %v", err)
	}
	if err := db.Create("b"); err != nil {
		t.Fatalf("Failed to create: %v", err)
	}
	if db.Use("a") == nil {
		t.Fatalf("a doesn't exist?!")
	}
	if db.Use("b") == nil {
		t.Fatalf("b doesn't exist?!")
	}
	// use
	if err := db.Use("a").BackupAndSaveConf(); err != nil {
		t.Fatalf("Use does not work")
	}
	// rename
	if err := db.Rename("a", "c"); err != nil {
		t.Fatal(err)
	}
	if _, nope := db.StrCol["a"]; nope {
		t.Fatalf("a still exists after it is renamed")
	}
	if _, ok := db.StrCol["c"]; !ok {
		t.Fatalf("c does not exist")
	}
	if err := db.Use("c").BackupAndSaveConf(); err != nil {
		t.Fatalf("c is unusable")
	}
	// drop
	if err := db.Drop("c"); err != nil {
		t.Fatal(err)
	}
	if _, nope := db.StrCol["c"]; nope {
		t.Fatalf("c still exists after it is deleted")
	}
}

func TestScrub(t *testing.T) {
	tmp := "/tmp/tiedot_db_test"
	os.RemoveAll(tmp)
	defer os.RemoveAll(tmp)
	db, err := OpenDB(tmp)
	if err != nil {
		t.Fatalf("Failed to open: %v", err)
		return
	}
	defer db.Close()
	// create
	if err := db.Create("a"); err != nil {
		t.Fatalf("Failed to create: %v", err)
	}
	// insert then delete
	docs := []string{`{"a": 1}`, `{"b": 2}`}
	var jsonDoc interface{}
	json.Unmarshal([]byte(docs[0]), &jsonDoc)
	db.Use("a").Index([]string{"a", "b", "c"})
	db.Use("a").Insert(jsonDoc)
	id1, _ := db.Use("a").Insert(jsonDoc)
	db.Use("a").Insert(jsonDoc)
	db.Use("a").Delete(id1)
	if err := db.Scrub("a"); err != nil {
		t.Fatal(err)
	}
	counter := 0
	db.Use("a").ForAll(func(id uint64, doc interface{}) bool {
		counter++
		return true
	})
	if counter != 2 {
		t.Fatalf("Scrub failure")
	}
	for k := range db.Use("a").StrHT {
		if k != "a,b,c" {
			t.Fatalf("Scrub did not recreate index")
		}
		break
	}
}
