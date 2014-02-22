/* Collection partition test cases. */
package colpart

import (
	"encoding/json"
	"fmt"
	"github.com/HouzuoGuo/tiedot/dstruct"
	"math/rand"
	"os"
	"strconv"
	"testing"
)

func TestHash(t *testing.T) {
	strings := []string{"", " ", "abc", "123"}
	hashes := []uint64{0, 32, 807794786, 408093746}
	for i := range strings {
		if StrHash(strings[i]) != hashes[i] {
			t.Fatalf("Hash of %s equals to %d, it should equal to %d", strings[i], StrHash(strings[i]), hashes[i])
		}
	}
}

func TestInsertRead(t *testing.T) {
	tmp := "/tmp/tiedot_col_test"
	os.RemoveAll(tmp)
	defer os.RemoveAll(tmp)
	col, err := OpenPart(tmp)
	if err != nil {
		t.Fatalf("Failed to open: %v", err)
		return
	}
	defer col.Close()
	docs := []string{`{"@id": "1", "a": 1}`, `{"@id": "2", "b": 2}`}
	var jsonDoc [2]map[string]interface{}
	json.Unmarshal([]byte(docs[0]), &jsonDoc[0])
	json.Unmarshal([]byte(docs[1]), &jsonDoc[1])

	ids := [2]uint64{}
	if ids[0], err = col.Insert(jsonDoc[0]); err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}
	if ids[1], err = col.Insert(jsonDoc[1]); err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}
	var doc1 interface{}
	if err = col.Read(ids[0], &doc1); doc1.(map[string]interface{})[string('a')].(float64) != 1.0 {
		t.Fatalf("Failed to read back document, got %v", doc1)
	}
	var doc2 interface{}
	err = col.Read(ids[1], &doc2)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(doc2, ids)
	if doc2.(map[string]interface{})[string('b')].(float64) != 2.0 {
		t.Fatalf("Failed to read back document, got %v", doc2)
	}
	if err = col.Flush(); err != nil {
		t.Fatal(err)
	}
}

func TestInsertUpdateReadAll(t *testing.T) {
	tmp := "/tmp/tiedot_col_test"
	os.RemoveAll(tmp)
	defer os.RemoveAll(tmp)
	col, err := OpenPart(tmp)
	if err != nil {
		t.Fatalf("Failed to open: %v", err)
		return
	}
	defer col.Close()

	docs := []string{`{"@id": "1", "a": 1}`, `{"@id": "1", "b": 2}`}
	var jsonDoc [2]map[string]interface{}
	json.Unmarshal([]byte(docs[0]), &jsonDoc[0])
	json.Unmarshal([]byte(docs[1]), &jsonDoc[1])

	updatedDocs := []string{`{"@id": "1", "a": 2}`, `{"@id": "1", "b": "abcdefghijklmnopqrstuvwxyz"}`}
	var updatedJsonDoc [2]map[string]interface{}
	json.Unmarshal([]byte(updatedDocs[0]), &updatedJsonDoc[0])
	json.Unmarshal([]byte(updatedDocs[1]), &updatedJsonDoc[1])

	ids := [2]uint64{}
	if ids[0], err = col.Insert(jsonDoc[0]); err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}
	if ids[1], err = col.Insert(jsonDoc[1]); err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	if ids[0], err = col.Update(ids[0], updatedJsonDoc[0]); err != nil {
		t.Fatalf("Failed to update: %v", err)
	}
	if ids[1], err = col.Update(ids[1], updatedJsonDoc[1]); err != nil {
		t.Fatalf("Failed to update: %v", err)
	}

	var doc1 interface{}
	if err = col.Read(ids[0], &doc1); doc1.(map[string]interface{})[string('a')].(float64) != 2.0 {
		t.Fatalf("Failed to read back document, got %v", doc1)
	}
	var doc2 interface{}
	if err = col.Read(ids[1], &doc2); doc2.(map[string]interface{})[string('b')].(string) != string("abcdefghijklmnopqrstuvwxyz") {
		t.Fatalf("Failed to read back document, got %v", doc2)
	}
	counter := 0
	col.ForAll(func(_ uint64, _ map[string]interface{}) bool {
		counter++
		return true
	})
	if counter != 2 {
		t.Fatalf("Expected to read 2 documents, but %d read", counter)
	}
	if err = col.Flush(); err != nil {
		t.Fatal(err)
	}
}

func TestInsertDeserialize(t *testing.T) {
	tmp := "/tmp/tiedot_col_test"
	os.RemoveAll(tmp)
	defer os.RemoveAll(tmp)
	col, err := OpenPart(tmp)
	if err != nil {
		t.Fatalf("Failed to open: %v", err)
		return
	}
	defer col.Close()

	var docs [2]map[string]interface{}
	if err = json.Unmarshal([]byte(`{"@id": "1", "I": 0, "S": "a", "B": false}`), &docs[0]); err != nil {
		panic(err)
	}
	if err = json.Unmarshal([]byte(`{"@id": "1", "I": 1, "S": "b", "B": true}`), &docs[1]); err != nil {
		panic(err)
	}

	ids := [2]uint64{}
	if ids[0], err = col.Insert(docs[0]); err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}
	if ids[1], err = col.Insert(docs[1]); err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	type Struct struct {
		I int
		S string
		B bool
	}
	template := new(Struct)
	col.DeserializeAll(template, func() bool {
		if !(template.I == 0 && template.S == "a" && template.B == false) && !(template.I == 1 && template.S == "b" && template.B == true) {
			t.Fatalf("Deserialized document is not expected: %v", template)
		}
		return true
	})
	if err = col.Flush(); err != nil {
		t.Fatal(err)
	}
}

func TestInsertDeleteRead(t *testing.T) {
	tmp := "/tmp/tiedot_col_test"
	os.RemoveAll(tmp)
	defer os.RemoveAll(tmp)
	col, err := OpenPart(tmp)
	if err != nil {
		t.Fatalf("Failed to open: %v", err)
		return
	}
	defer col.Close()
	docs := []string{`{"@id": "1", "a": 1}`, `{"@id": "1", "b": 2}`}
	var jsonDoc [2]map[string]interface{}
	json.Unmarshal([]byte(docs[0]), &jsonDoc[0])
	json.Unmarshal([]byte(docs[1]), &jsonDoc[1])
	ids := [2]uint64{}
	if ids[0], err = col.Insert(jsonDoc[0]); err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}
	if ids[1], err = col.Insert(jsonDoc[1]); err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}
	col.Delete(ids[0])
	var doc1 interface{}
	if err = col.Read(ids[0], &doc1); doc1 != nil {
		t.Fatalf("Did not delete document")
	}
	var doc2 interface{}
	if err = col.Read(ids[1], &doc2); doc2.(map[string]interface{})[string('b')].(float64) != 2 {
		t.Fatalf("Failed to read back document, got %v", doc2)
	}
}

func TestScrubAndColScan(t *testing.T) {
	tmp := "/tmp/tiedot_col_test"
	os.RemoveAll(tmp)
	defer os.RemoveAll(tmp)
	col, err := OpenPart(tmp)
	if err != nil {
		t.Fatalf("Failed to open: %v", err)
		return
	}
	// Insert 10000 documents
	var doc map[string]interface{}
	json.Unmarshal([]byte(`{"@id": "`+strconv.Itoa(rand.Intn(10000))+`", "a": [{"b": {"c": [4]}}, {"b": {"c": [5, 6]}}], "d": [0, 9]}`), &doc)
	for i := 0; i < 10000; i++ {
		_, err := col.Insert(doc)
		if err != nil {
			t.Fatal("Insert fault")
		}
	}
	// Do some serious damage to index and collection data
	for i := 0; i < 1024*1024*1; i++ {
		col.PK.File.Buf[i] = 6
	}
	for i := 1024; i < 1024*128; i++ {
		col.Data.File.Buf[i] = 6
	}
	for i := 1024 * 256; i < 1024*512; i++ {
		col.Data.File.Buf[i] = 6
	}
	if err = col.Flush(); err != nil {
		t.Fatal(err)
	}
	if err = col.Flush(); err != nil {
		t.Fatal(err)
	}
	col.Close()
}

func IndexContainsAll(index *dstruct.HashTable, expectedKV map[uint64]uint64) bool {
	keys, vals := index.GetAll(0)
	kvMap := make(map[uint64]uint64)
	for i, key := range keys {
		kvMap[key] = vals[i]
	}
	fmt.Printf("Comparing %v with %v\n", kvMap, expectedKV)
	for key, val := range expectedKV {
		if kvMap[key] != val {
			return false
		}
	}
	return true
}

func TestIndexAndReopen(t *testing.T) {
	tmp := "/tmp/tiedot_col_test"
	os.RemoveAll(tmp)
	defer os.RemoveAll(tmp)
	col, err := OpenPart(tmp)
	if err != nil {
		t.Fatalf("Failed to open: %v", err)
		return
	}
	docs := []string{
		`{"@id": "1", "a": {"b": {"c": 1}}, "d": 0}`,
		`{"@id": "2", "a": {"b": [{"c": 2}]}, "d": 0}`,
		`{"@id": "3", "a": [{"b": {"c": 3}}], "d": 0}`,
		`{"@id": "4", "a": [{"b": {"c": [4]}}, {"b": {"c": [5, 6]}}], "d": [0, 9]}`,
		`{"@id": "5", "a": {"b": {"c": null}}, "d": null}`}
	var jsonDoc [5]map[string]interface{}

	json.Unmarshal([]byte(docs[0]), &jsonDoc[0])
	json.Unmarshal([]byte(docs[1]), &jsonDoc[1])
	json.Unmarshal([]byte(docs[2]), &jsonDoc[2])
	json.Unmarshal([]byte(docs[3]), &jsonDoc[3])
	json.Unmarshal([]byte(docs[4]), &jsonDoc[4])
	ids := [3]uint64{}
	// Insert first document
	ids[0], _ = col.Insert(jsonDoc[0])
	// There should be one document on index - the first doc
	if !IndexContainsAll(col.PK, map[uint64]uint64{1: ids[0]}) {
		t.Fatal()
	}
	// Insert second and third document, replace third document by forth document
	ids[1], _ = col.Insert(jsonDoc[1])
	ids[2], _ = col.Insert(jsonDoc[2])
	ids[2], _ = col.Update(ids[2], jsonDoc[3])
	// Then remove second document
	col.Delete(ids[1])
	// jsonDoc[0,3], ids[0, 3] are the ones left
	if !IndexContainsAll(col.PK, map[uint64]uint64{1: ids[0], 4: ids[2]}) {
		t.Fatal()
	}
	// Reopen the collection and continue testing indexes
	col.Close()
	col, err = OpenPart(tmp)
	if err != nil {
		t.Fatal(err)
	}
	if !IndexContainsAll(col.PK, map[uint64]uint64{1: ids[0], 4: ids[2]}) {
		t.Fatal()
	}
	// Insert a new document and try index
	newID, err := col.Insert(jsonDoc[4])
	if err != nil {
		t.Fatal("insert error")
	}
	if !IndexContainsAll(col.PK, map[uint64]uint64{1: ids[0], 4: ids[2], 5: newID}) {
		t.Fatal("Index failure")
	}
	// Try ID to physical ID conversion
	if physID, err := col.GetPhysicalID(1); physID != ids[0] || err != nil {
		t.Fatal(err, physID, ids[0])
	}
	if physID, err := col.GetPhysicalID(4); physID != ids[2] || err != nil {
		t.Fatal(err, physID)
	}
	if physID, err := col.GetPhysicalID(5); physID != newID || err != nil {
		t.Fatal(err, physID)
	}
	if err = col.Flush(); err != nil {
		t.Fatal(err)
	}
	col.Close()
}
