/* Database test cases. */
package db

import (
	"encoding/json"
	"github.com/HouzuoGuo/tiedot/chunkfile"
	"os"
	"strconv"
	"strings"
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
	col := db.Use("a")
	// Insert 10 long documents, afterwards there should be 5 chunks
	longDocIDs := make([]uint64, 10)
	for i := 0; i < 10; i++ {
		var longDoc interface{}
		json.Unmarshal([]byte(`{"a": "`+strings.Repeat("1", int(chunkfile.DOC_MAX_ROOM/6))+`", "id": `+strconv.Itoa(i)+`}`), &longDoc)
		if longDocIDs[i], err = col.Insert(longDoc); err != nil {
			t.Fatal(err)
		}
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
	colc := db.Use("c")
	// use renamed
	for i := 0; i < 10; i++ {
		var longDoc interface{}
		json.Unmarshal([]byte(`{"a": "`+strings.Repeat("1", int(chunkfile.DOC_MAX_ROOM/6))+`", "id": `+strconv.Itoa(i)+`}`), &longDoc)
		if longDocIDs[i], err = colc.Insert(longDoc); err != nil {
			t.Fatal(err)
		}
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
}
