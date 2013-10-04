package db

import (
	"encoding/json"
	"fmt"
	"os"
	"testing"
)

const COL_BENCH_SIZE = 200000 // Number of documents made available for collection benchmark

func TestInsertRead(t *testing.T) {
	tmp := "/tmp/tiedot_col_test"
	os.RemoveAll(tmp)
	defer os.RemoveAll(tmp)
	col, err := OpenCol(tmp)
	if err != nil {
		t.Fatalf("Failed to open: %v", err)
		return
	}
	defer col.Close()
	docs := []string{`{"a": 1}`, `{"b": 2}`}
	var jsonDoc [2]interface{}
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
	if err = col.Read(ids[1], &doc2); doc2.(map[string]interface{})[string('b')].(float64) != 2.0 {
		t.Fatalf("Failed to read back document, got %v", doc2)
	}
}

func TestInsertUpdateReadAll(t *testing.T) {
	tmp := "/tmp/tiedot_col_test"
	os.RemoveAll(tmp)
	defer os.RemoveAll(tmp)
	col, err := OpenCol(tmp)
	if err != nil {
		t.Fatalf("Failed to open: %v", err)
		return
	}
	defer col.Close()

	docs := []string{`{"a": 1}`, `{"b": 2}`}
	var jsonDoc [2]interface{}
	json.Unmarshal([]byte(docs[0]), &jsonDoc[0])
	json.Unmarshal([]byte(docs[1]), &jsonDoc[1])

	updatedDocs := []string{`{"a": 2}`, `{"b": "abcdefghijklmnopqrstuvwxyz"}`}
	var updatedJsonDoc [2]interface{}
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
	col.ForAll(func(id uint64, doc interface{}) bool {
		counter++
		return true
	})
	if counter != 2 {
		t.Fatalf("Expected to read 2 documents, but %d read", counter)
	}
}

func TestInsertDeserialize(t *testing.T) {
	tmp := "/tmp/tiedot_col_test"
	os.RemoveAll(tmp)
	defer os.RemoveAll(tmp)
	col, err := OpenCol(tmp)
	if err != nil {
		t.Fatalf("Failed to open: %v", err)
		return
	}
	defer col.Close()

	type Struct struct {
		I int
		S string
		B bool
	}

	doc0 := &Struct{0, "a", false}
	doc1 := &Struct{1, "b", true}

	ids := [2]uint64{}
	if ids[0], err = col.Insert(doc0); err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}
	if ids[1], err = col.Insert(doc1); err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	template := new(Struct)
	col.DeserializeAll(template, func(id uint64) bool {
		switch id {
		case ids[0]:
			if !(template.I == 0 && template.S == "a" && template.B == false) {
				t.Fatalf("Deserialized document is not expected: %v", template)
			}
		case ids[1]:
			if !(template.I == 1 && template.S == "b" && template.B == true) {
				t.Fatalf("Deserialized document is not expected: %v", template)
			}
		}
		return true
	})
}

func TestDurableInsertUpdateDelete(t *testing.T) {
	tmp := "/tmp/tiedot_col_test"
	os.RemoveAll(tmp)
	defer os.RemoveAll(tmp)
	col, err := OpenCol(tmp)
	if err != nil {
		t.Fatalf("Failed to open: %v", err)
		return
	}
	defer col.Close()

	docs := []string{`{"a": 1}`, `{"b": 2}`}
	var jsonDoc [2]interface{}
	json.Unmarshal([]byte(docs[0]), &jsonDoc[0])
	json.Unmarshal([]byte(docs[1]), &jsonDoc[1])

	updatedDocs := []string{`{"a": 2}`, `{"b": "abcdefghijklmnopqrstuvwxyz"}`}
	var updatedJsonDoc [2]interface{}
	json.Unmarshal([]byte(updatedDocs[0]), &updatedJsonDoc[0])
	json.Unmarshal([]byte(updatedDocs[1]), &updatedJsonDoc[1])

	ids := [2]uint64{}
	if ids[0], err = col.DurableInsert(jsonDoc[0]); err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}
	if ids[1], err = col.DurableInsert(jsonDoc[1]); err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	if ids[0], err = col.DurableUpdate(ids[0], updatedJsonDoc[0]); err != nil {
		t.Fatalf("Failed to update: %v", err)
	}
	if ids[1], err = col.DurableUpdate(ids[1], updatedJsonDoc[1]); err != nil {
		t.Fatalf("Failed to update: %v", err)
	}
	if err = col.DurableDelete(12345); err != nil {
		t.Fatalf("Failed to delete: %v", err)
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
	col.ForAll(func(id uint64, doc interface{}) bool {
		counter++
		return true
	})
	if counter != 2 {
		t.Fatalf("Expected to read 2 documents, but %d read", counter)
	}
}

func TestInsertDeleteRead(t *testing.T) {
	tmp := "/tmp/tiedot_col_test"
	os.RemoveAll(tmp)
	defer os.RemoveAll(tmp)
	col, err := OpenCol(tmp)
	if err != nil {
		t.Fatalf("Failed to open: %v", err)
		return
	}
	defer col.Close()
	docs := []string{`{"a": 1}`, `{"b": 2}`}
	var jsonDoc [2]interface{}
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

func TestIndex(t *testing.T) {
	tmp := "/tmp/tiedot_col_test"
	os.RemoveAll(tmp)
	defer os.RemoveAll(tmp)
	col, err := OpenCol(tmp)
	if err != nil {
		t.Fatalf("Failed to open: %v", err)
		return
	}
	defer col.Close()
	docs := []string{
		`{"a": {"b": {"c": 1}}, "d": 0}`,
		`{"a": {"b": {"c": 2}}, "d": 0}`,
		`{"a": {"b": {"c": 3}}, "d": 0}`,
		`{"a": {"b": {"c": 4}}, "d": [0, 9]}`,
		`{"a": {"b": {"c": null}}, "d": null}`}
	var jsonDoc [4]interface{}
	json.Unmarshal([]byte(docs[0]), &jsonDoc[0])
	json.Unmarshal([]byte(docs[1]), &jsonDoc[1])
	json.Unmarshal([]byte(docs[2]), &jsonDoc[2])
	json.Unmarshal([]byte(docs[3]), &jsonDoc[3])
	ids := [3]uint64{}
	ids[0], _ = col.Insert(jsonDoc[0])
	if err = col.Index([]string{"a", "b", "c"}); err != nil {
		t.Fatal(err)
		return
	}
	if err = col.Index([]string{"d"}); err != nil {
		t.Fatal(err)
		return
	}
	keys, vals := col.StrHT["a,b,c"].GetAll(0)
	if !(len(keys) == 1 && len(vals) == 1 && vals[0] == ids[0]) {
		t.Fatalf("Did not index existing document, got %v, %v", keys, vals)
	}
	ids[1], _ = col.Insert(jsonDoc[1])
	ids[2], _ = col.Insert(jsonDoc[2])
	ids[2], _ = col.Update(ids[2], jsonDoc[3])
	col.Delete(ids[1])
	// jsonDoc[0,3], ids[0, 2] are the ones left
	index1 := col.StrHT["a,b,c"]
	index2 := col.StrHT["d"]
	// d index
	k0, v0 := index2.Get(StrHash(0), 0, func(k, v uint64) bool {
		return true
	})
	k9, v9 := index2.Get(StrHash(9), 0, func(k, v uint64) bool {
		return true
	})
	if !(len(k0) == 2 && len(v0) == 2 && k0[0] == StrHash(0) && v0[0] == ids[0] && k0[1] == StrHash(0) && v0[1] == ids[2]) {
		t.Fatalf("Index fault on key 0, %v, %v", k0, v0)
	}
	if !(len(k9) == 1 && len(v9) == 1 && k9[0] == StrHash(9) && v9[0] == ids[2]) {
		t.Fatalf("Index fault on key 9, %v, %v", k9, v9)
	}
	// abc index
	k1, v1 := index1.Get(StrHash(1), 0, func(k, v uint64) bool {
		return true
	})
	k4, v4 := index1.Get(StrHash(4), 0, func(k, v uint64) bool {
		return true
	})
	if !(len(k1) == 1 && len(v1) == 1 && k1[0] == StrHash(1) && v1[0] == ids[0]) {
		t.Fatalf("Index fault, %v, %v", k1, v1)
	}
	if !(len(k4) == 1 && len(v4) == 1 && k4[0] == StrHash(4) && v4[0] == ids[2]) {
		t.Fatalf("Index fault, %v, %v", k4, v4)
	}
	// now remove a,b,c index
	if err = col.Unindex([]string{"a", "b", "c"}); err != nil {
		t.Fatal(err)
		return
	}
	if _, ok := col.StrHT["a,b,c"]; ok {
		t.Fatal("did not delete index")
	}
	if _, ok := col.StrIC["a,b,c"]; ok {
		t.Fatal("did not delete index")
	}
	newID, err := col.Insert(jsonDoc[0])
	if err != nil {
		t.Fatal(err)
	}
	k0, v0 = col.StrHT["d"].Get(StrHash(0), 0, func(k, v uint64) bool {
		return true
	})
	if !(len(k0) == 3 && len(v0) == 3 && k0[0] == StrHash(0) && v0[0] == ids[0] && k0[1] == StrHash(0) && v0[2] == ids[2] && k0[2] == StrHash(0) && v0[1] == newID) {
		t.Fatalf("Index fault, %d, %v, %v", newID, k0, v0)
	}
}

func TestUIDDocCRUD(t *testing.T) {
	tmp := "/tmp/tiedot_col_test"
	os.RemoveAll(tmp)
	defer os.RemoveAll(tmp)
	col, err := OpenCol(tmp)
	if err != nil {
		t.Fatalf("Failed to open: %v", err)
		return
	}
	defer col.Close()
	docs := []string{
		`{"a": {"b": {"c": 1}}, "d": 1}`,
		`{"a": {"b": {"c": 2}}, "d": 2}`,
		`{"a": {"b": {"c": 3}}, "d": 3}`}
	var jsonDocs [3]interface{}
	var ids [3]uint64
	var uids [3]string
	json.Unmarshal([]byte(docs[0]), &jsonDocs[0])
	json.Unmarshal([]byte(docs[1]), &jsonDocs[1])
	json.Unmarshal([]byte(docs[2]), &jsonDocs[2])
	// insert
	ids[0], uids[0], err = col.InsertWithUID(jsonDocs[0])
	if err != nil {
		t.Fatal(err)
	}
	ids[1], uids[1], err = col.InsertWithUID(jsonDocs[1])
	if err != nil {
		t.Fatal(err)
	}
	ids[2], uids[2], err = col.InsertWithUID(jsonDocs[2])
	if err != nil {
		t.Fatal(err)
	}
	if len(uids[0]) != 32 || len(uids[1]) != 32 || len(uids[2]) != 32 ||
		uids[0] == uids[1] || uids[1] == uids[2] || uids[2] == uids[0] ||
		ids[0] == ids[1] || ids[1] == ids[2] || ids[2] == ids[0] {
		t.Fatalf("Malformed UIDs or IDs: %v %v", uids, ids)
	}
	// read
	var readDoc interface{}
	readID, readErr := col.ReadByUID(uids[1], &readDoc)
	if readErr != nil {
		t.Fatal(readErr)
	}
	docMap1 := readDoc.(map[string]interface{})
	docMap2 := jsonDocs[1].(map[string]interface{})
	if readID != ids[1] || fmt.Sprint(docMap1["a"]) != fmt.Sprint(docMap2["a"]) {
		t.Fatalf("Cannot read back original document by UID: %v", readDoc)
	}
	// update
	var docWithoutUID interface{}
	json.Unmarshal([]byte(docs[1]), &docWithoutUID)
	if _, err := col.UpdateByUID(uids[0], docWithoutUID); err != nil { // intentionally remove UID
		t.Fatal(err)
	}
	if _, err = col.ReadByUID(uids[0], &readDoc); err == nil { // UID was removed therefore the error shall happen
		t.Fatalf("UpdateByUID did not work, still read %v", readDoc)
	}
	// update (reassign UID)
	newID, newUID, err := col.ReassignUID(ids[0])
	if newID != ids[0] || len(newUID) != 32 || err != nil {
		t.Fatalf("ReassignUID did not work: %v %v %v", newID, newUID, err)
	}
	uids[0] = newUID
	if _, err = col.ReadByUID(uids[0], &readDoc); err != nil { // UID was reassigned, the error shall NOT happen
		t.Fatalf("ReassignUID did not work")
	}
	// delete
	col.DeleteByUID(uids[0])
	if _, err = col.ReadByUID(uids[0], &readDoc); err == nil {
		t.Fatalf("DeleteByUID did not work")
	}
}
