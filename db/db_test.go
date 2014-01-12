/* Database test cases. */
package db

import (
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
	// create
	if err := db.Create("a", 2); err != nil {
		t.Fatalf("Failed to create: %v", err)
	}
	if err := db.Create("b", 3); err != nil {
		t.Fatalf("Failed to create: %v", err)
	}
	if db.Use("a") == nil {
		t.Fatalf("a doesn't exist?!")
	}
	if db.Use("b") == nil {
		t.Fatalf("b doesn't exist?!")
	}
	// use
	col := db.Use("a")
	if err = col.Index([]string{"a"}); err != nil {
		t.Fatal(err)
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
	col = db.Use("c")
	// use renamed
	if err = col.Index([]string{"b"}); err != nil {
		t.Fatal(err)
	}
	// drop
	if err := db.Drop("c"); err != nil {
		t.Fatal(err)
	}
	if _, nope := db.StrCol["c"]; nope {
		t.Fatalf("c still exists after it is deleted")
	}
	// flush & close
	db.Flush()
	db.Close()
	// use reopened
	db, err = OpenDB(tmp)
	if err != nil {
		t.Fatalf("Failed to open: %v", err)
		return
	}
	if err = db.Use("b").Index([]string{"c"}); err != nil {
		t.Fatal(err)
	}
	// reopen and verify chunk number
	if db.Use("b").NumChunks != 3 {
		t.Fatal()
	}
}
