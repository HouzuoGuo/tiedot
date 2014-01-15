/* Database test cases. */
package db

import (
	"encoding/json"
	"fmt"
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
	// scrub, flush and reopen
	if _, err = db.Scrub("c"); err != nil {
		t.Fatal(err)
	}
	db.Flush()
	db.Close()
	db, err = OpenDB(tmp)
	if err != nil {
		t.Fatalf("Failed to open: %v", err)
		return
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

func TestScrub(t *testing.T) {
	tmp := "/tmp/tiedot_db_test"
	os.RemoveAll(tmp)
	defer os.RemoveAll(tmp)
	db, err := OpenDB(tmp)
	if err != nil {
		t.Fatalf("Failed to open: %v", err)
		return
	}
	// Create and use collection A
	db.Create("a", 2)
	col := db.Use("a")
	// Create two indexes
	if err = col.Index([]string{"a", "b", "c"}); err != nil {
		t.Fatal(err)
		return
	}
	if err = col.Index([]string{"d"}); err != nil {
		t.Fatal(err)
		return
	}
	// Insert 10000 documents
	var doc map[string]interface{}
	json.Unmarshal([]byte(`{"a": [{"b": {"c": [4]}}, {"b": {"c": [5, 6]}}], "d": [0, 9]}`), &doc)
	for i := 0; i < 10000; i++ {
		_, err := col.Insert(doc)
		if err != nil {
			t.Fatal("Insert fault")
		}
	}
	// Do some serious damage to index and collection data
	for i := 0; i < 1024*1024*1; i++ {
		col.Chunks[0].PK.File.Buf[i] = 6
	}
	for i := 0; i < 1024*1024*1; i++ {
		col.Chunks[1].PK.File.Buf[i] = 6
	}
	for i := 1024 * 512; i < 1024*1024; i++ {
		col.Chunks[0].Data.File.Buf[i] = 6
	}
	for i := 1024 * 512; i < 1024*1024; i++ {
		col.Chunks[1].Data.File.Buf[i] = 6
	}
	for i := 1024; i < 1024*128; i++ {
		col.SecIndexes["a,b,c"][0].File.Buf[i] = 6
	}
	for i := 1024; i < 1024*128; i++ {
		col.SecIndexes["d"][1].File.Buf[i] = 6
	}
	db.Flush()
	db.Close()
	// Reopen the chunk and expect data structure failure messages from log
	fmt.Println("Please ignore the following error messages")
	reopen, err := OpenDB(tmp)
	if err != nil {
		t.Fatal(err)
	}
	recoveredNum, err := reopen.Scrub("a")
	if err != nil {
		t.Fatal(err)
	}
	// Confirm that 6212 documents are successfully recovered in four ways
	counter := 0
	// first - deserialization & scan
	var recoveredDoc interface{}
	reopen.Use("a").DeserializeAll(&recoveredDoc, func() bool {
		counter++
		return true
	})
	if counter != 6212 {
		t.Fatal("Did not recover enough documents", counter)
	}
	// second - collection scan
	counter = 0
	reopen.Use("a").ForAll(func(id int, doc map[string]interface{}) bool {
		counter++
		return true
	})
	if counter != 6212 {
		t.Fatal("Did not recover enough documents")
	}
	// third - index scan
	keys1, vals1 := reopen.Use("a").SecIndexes["a,b,c"][0].GetAll(0)
	keys2, vals2 := reopen.Use("a").SecIndexes["a,b,c"][1].GetAll(0)
	if !(len(keys1)+len(keys2) == 6212*3 && len(vals1)+len(vals2) == 6212*3) {
		t.Fatalf("Did not recover enough documents on index, got only %d", len(vals1)+len(vals2))
	}
	keys3, vals3 := reopen.Use("a").SecIndexes["d"][1].GetAll(0)
	keys4, vals4 := reopen.Use("a").SecIndexes["d"][0].GetAll(0)
	if !(len(keys3)+len(keys4) == 6212*2 && len(vals3)+len(vals4) == 6212*2) {
		t.Fatalf("Did not recover enough documents on index, got only %d", len(vals1)+len(vals2))
	}

	// fourth - scrub return value
	if recoveredNum != 6212 {
		t.Fatal("Scrub return value is wrong")
	}
}

func TestRepartition(t *testing.T) {
	tmp := "/tmp/tiedot_db_test"
	os.RemoveAll(tmp)
	defer os.RemoveAll(tmp)
	db, err := OpenDB(tmp)
	if err != nil {
		t.Fatalf("Failed to open: %v", err)
		return
	}
	// Create and use collection A
	db.Create("a", 4)
	col := db.Use("a")
	// Create two indexes
	if err = col.Index([]string{"a", "b", "c"}); err != nil {
		t.Fatal(err)
		return
	}
	if err = col.Index([]string{"d"}); err != nil {
		t.Fatal(err)
		return
	}
	// Insert 10000 documents
	var doc map[string]interface{}
	json.Unmarshal([]byte(`{"a": [{"b": {"c": [4]}}, {"b": {"c": [5, 6]}}], "d": [0, 9]}`), &doc)
	for i := 0; i < 10000; i++ {
		_, err := col.Insert(doc)
		if err != nil {
			t.Fatal("Insert fault")
		}
	}
	// Do some serious damage to index and collection data
	for i := 0; i < 1024*1024*1; i++ {
		col.Chunks[0].PK.File.Buf[i] = 6
	}
	for i := 0; i < 1024*1024*1; i++ {
		col.Chunks[1].PK.File.Buf[i] = 6
	}
	for i := 1024 * 256; i < 1024*1024; i++ {
		col.Chunks[0].Data.File.Buf[i] = 6
	}
	for i := 1024 * 256; i < 1024*1024; i++ {
		col.Chunks[1].Data.File.Buf[i] = 6
	}
	for i := 1024; i < 1024*128; i++ {
		col.SecIndexes["a,b,c"][0].File.Buf[i] = 6
	}
	for i := 1024; i < 1024*128; i++ {
		col.SecIndexes["d"][1].File.Buf[i] = 6
	}
	db.Flush()
	db.Close()
	// Reopen the chunk and expect data structure failure messages from log
	fmt.Println("Please ignore the following error messages")
	reopen, err := OpenDB(tmp)
	if err != nil {
		t.Fatal(err)
	}
	recoveredNum, err := reopen.Repartition("a", 2)
	if err != nil {
		t.Fatal(err)
	}
	// Confirm that 8110 documents are successfully recovered in four ways
	counter := 0
	// first - deserialization & scan
	var recoveredDoc interface{}
	reopen.Use("a").DeserializeAll(&recoveredDoc, func() bool {
		counter++
		return true
	})
	if counter != 8110 {
		t.Fatal("Did not recover enough documents", counter)
	}
	// second - collection scan
	counter = 0
	reopen.Use("a").ForAll(func(id int, doc map[string]interface{}) bool {
		counter++
		return true
	})
	if counter != 8110 {
		t.Fatal("Did not recover enough documents")
	}
	// third - index scan
	keys1, vals1 := reopen.Use("a").SecIndexes["a,b,c"][0].GetAll(0)
	keys2, vals2 := reopen.Use("a").SecIndexes["a,b,c"][1].GetAll(0)
	if !(len(keys1)+len(keys2) == 8110*3 && len(vals1)+len(vals2) == 8110*3) {
		t.Fatalf("Did not recover enough documents on index, got only %d", len(vals1)+len(vals2))
	}
	keys3, vals3 := reopen.Use("a").SecIndexes["d"][1].GetAll(0)
	keys4, vals4 := reopen.Use("a").SecIndexes["d"][0].GetAll(0)
	if !(len(keys3)+len(keys4) == 8110*2 && len(vals3)+len(vals4) == 8110*2) {
		t.Fatalf("Did not recover enough documents on index, got only %d", len(vals1)+len(vals2))
	}

	// fourth - scrub return value
	if recoveredNum != 8110 {
		t.Fatal("Scrub return value is wrong")
	}
}
