package chunk

import (
	"encoding/json"
	"fmt"
	"github.com/HouzuoGuo/tiedot/chunkfile"
	"os"
	"strings"
	"testing"
)

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
	col, err := OpenChunk(0, tmp)
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
	var outOfSpace bool
	if ids[0], outOfSpace, err = col.Insert(jsonDoc[0]); err != nil || outOfSpace {
		t.Fatalf("Failed to insert: %v", err)
	}
	if ids[1], outOfSpace, err = col.Insert(jsonDoc[1]); err != nil {
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
	col, err := OpenChunk(0, tmp)
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
	var outOfSpace bool
	if ids[0], outOfSpace, err = col.Insert(jsonDoc[0]); err != nil || outOfSpace {
		t.Fatalf("Failed to insert: %v", err)
	}
	if ids[1], outOfSpace, err = col.Insert(jsonDoc[1]); err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	if ids[0], outOfSpace, err = col.Update(ids[0], updatedJsonDoc[0]); err != nil || outOfSpace {
		t.Fatalf("Failed to update: %v", err)
	}
	if ids[1], outOfSpace, err = col.Update(ids[1], updatedJsonDoc[1]); err != nil || outOfSpace {
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
	col, err := OpenChunk(0, tmp)
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
	var outOfSpace bool
	if ids[0], outOfSpace, err = col.Insert(doc0); err != nil || outOfSpace {
		t.Fatalf("Failed to insert: %v", err)
	}
	if ids[1], outOfSpace, err = col.Insert(doc1); err != nil || outOfSpace {
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

func TestInsertDeleteRead(t *testing.T) {
	tmp := "/tmp/tiedot_col_test"
	os.RemoveAll(tmp)
	defer os.RemoveAll(tmp)
	col, err := OpenChunk(0, tmp)
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
	var outOfSpace bool
	if ids[0], outOfSpace, err = col.Insert(jsonDoc[0]); err != nil || outOfSpace {
		t.Fatalf("Failed to insert: %v", err)
	}
	if ids[1], outOfSpace, err = col.Insert(jsonDoc[1]); err != nil || outOfSpace {
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

func TestIndexAndReopen(t *testing.T) {
	tmp := "/tmp/tiedot_col_test"
	os.RemoveAll(tmp)
	defer os.RemoveAll(tmp)
	col, err := OpenChunk(0, tmp)
	if err != nil {
		t.Fatalf("Failed to open: %v", err)
		return
	}
	docs := []string{
		`{"a": {"b": {"c": 1}}, "d": 0}`,
		`{"a": {"b": [{"c": 2}]}, "d": 0}`,
		`{"a": [{"b": {"c": 3}}], "d": 0}`,
		`{"a": [{"b": {"c": [4]}}, {"b": {"c": [5, 6]}}], "d": [0, 9]}`,
		`{"a": {"b": {"c": null}}, "d": null}`}
	var jsonDoc [4]interface{}
	var outOfSpace bool
	json.Unmarshal([]byte(docs[0]), &jsonDoc[0])
	json.Unmarshal([]byte(docs[1]), &jsonDoc[1])
	json.Unmarshal([]byte(docs[2]), &jsonDoc[2])
	json.Unmarshal([]byte(docs[3]), &jsonDoc[3])
	ids := [3]uint64{}
	// Insert first document
	ids[0], _, _ = col.Insert(jsonDoc[0])
	if err = col.Index([]string{"a", "b", "c"}); err != nil {
		t.Fatal(err)
		return
	}
	if err = col.Index([]string{"d"}); err != nil {
		t.Fatal(err)
		return
	}
	// The index should be in structures
	if len(col.Path2HT) != 3 || len(col.HTPaths) != 3 || len(col.Hashtables) != 3 {
		t.Fatalf("index fault %v", col.Path2HT)
	}
	if col.HTPaths[0][0] != UID_PATH {
		t.Fatal("index fault")
	}
	if col.HTPaths[1][0] != "a" || col.HTPaths[1][1] != "b" || col.HTPaths[1][2] != "c" {
		t.Fatal("index fault")
	}
	if col.HTPaths[2][0] != "d" {
		t.Fatal("index fault")
	}
	// There should be one document on index - the first doc
	keys, vals := col.Path2HT["a,b,c"].GetAll(0)
	if !(len(keys) == 1 && len(vals) == 1 && vals[0] == ids[0]) {
		t.Fatalf("Did not index existing document, got %v, %v", keys, vals)
	}
	// Insert second and third document, replace third document by fouth document
	ids[1], _, _ = col.Insert(jsonDoc[1])
	ids[2], _, _ = col.Insert(jsonDoc[2])
	ids[2], _, _ = col.Update(ids[2], jsonDoc[3])
	// Then remove second document
	col.Delete(ids[1])
	// jsonDoc[0,3], ids[0, 2] are the ones left
	index1 := col.Path2HT["a,b,c"]
	index2 := col.Path2HT["d"]
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
	// abc index and d index should contain correct number of values
	keys, vals = col.Path2HT["a,b,c"].GetAll(0)
	if !(len(keys) == 4 && len(vals) == 4) { // doc 0, 3
		t.Fatalf("Index has too many values: %d, %d", keys, vals)
	}
	keys, vals = col.Path2HT["d"].GetAll(0)
	if !(len(keys) == 3 && len(vals) == 3) { // doc 0, 3
		t.Fatal("Index has too many values")
	}
	// abc index
	k1, v1 := index1.Get(StrHash(1), 0, func(k, v uint64) bool {
		return true
	})
	k2, v2 := index1.Get(StrHash(2), 0, func(k, v uint64) bool {
		return true
	})
	k4, v4 := index1.Get(StrHash(4), 0, func(k, v uint64) bool {
		return true
	})
	k5, v5 := index1.Get(StrHash(5), 0, func(k, v uint64) bool {
		return true
	})
	k6, v6 := index1.Get(StrHash(6), 0, func(k, v uint64) bool {
		return true
	})
	if !(len(k1) == 1 && len(v1) == 1 && k1[0] == StrHash(1) && v1[0] == ids[0]) {
		t.Fatalf("Index fault, %v, %v", k1, v1)
	}
	if !(len(k2) == 0 && len(v2) == 0) {
		t.Fatalf("Index fault, %v, %v", k2, v2)
	}
	if !(len(k4) == 1 && len(v4) == 1 && k4[0] == StrHash(4) && v4[0] == ids[2]) {
		t.Fatalf("Index fault, %v, %v", k4, v4)
	}
	if !(len(k5) == 1 && len(v5) == 1 && k5[0] == StrHash(5) && v5[0] == ids[2]) {
		t.Fatalf("Index fault, %v, %v", k5, v5)
	}
	if !(len(k6) == 1 && len(v6) == 1 && k6[0] == StrHash(6) && v6[0] == ids[2]) {
		t.Fatalf("Index fault, %v, %v", k6, v6)
	}
	// Reopen the collection and test number of indexes
	col.Close()
	col, err = OpenChunk(0, tmp)
	if err != nil {
		t.Fatal(err)
	}
	if !(len(col.Path2HT) == 3 && len(col.Hashtables) == 3 && len(col.HTPaths) == 3) {
		t.Fatal("Did not reopen 3 indexes")
	}
	// Now remove a,b,c index
	if err = col.Unindex([]string{"a", "b", "c"}); err != nil {
		t.Fatal(err)
		return
	}
	if _, ok := col.Path2HT["a,b,c"]; ok {
		t.Fatal("did not delete index")
	}
	if len(col.Path2HT) != 2 || len(col.Hashtables) != 2 || len(col.HTPaths) != 2 {
		t.Fatal("did not delete index")
	}
	if col.HTPaths[0][0] != UID_PATH {
		t.Fatal("index fault")
	}
	if col.HTPaths[1][0] != "d" {
		t.Fatal("index fault")
	}
	newID, outOfSpace, err := col.Insert(jsonDoc[0])
	if err != nil || outOfSpace {
		t.Fatal("insert error")
	}
	k0, v0 = col.Path2HT["d"].Get(StrHash(0), 0, func(k, v uint64) bool {
		return true
	})
	if !(len(k0) == 3 && len(v0) == 3 && k0[0] == StrHash(0) && v0[0] == ids[0] && k0[1] == StrHash(0) && v0[2] == ids[2] && k0[2] == StrHash(0) && v0[1] == newID) {
		t.Fatalf("Index fault, %d, %v, %v", newID, k0, v0)
	}
	col.Close()
}

func TestUIDDocCRUDAndReopen(t *testing.T) {
	tmp := "/tmp/tiedot_col_test"
	os.RemoveAll(tmp)
	defer os.RemoveAll(tmp)
	col, err := OpenChunk(0, tmp)
	if err != nil {
		t.Fatalf("Failed to open: %v", err)
		return
	}
	docs := []string{
		`{"a": {"b": {"c": 1}}, "d": 1}`,
		`{"a": {"b": {"c": 2}}, "d": 2}`,
		`{"a": {"b": {"c": 3}}, "d": 3}`}
	var outOfSpace bool
	var jsonDocs [3]interface{}
	var ids [3]uint64
	var uids [3]string
	json.Unmarshal([]byte(docs[0]), &jsonDocs[0])
	json.Unmarshal([]byte(docs[1]), &jsonDocs[1])
	json.Unmarshal([]byte(docs[2]), &jsonDocs[2])
	// insert
	ids[0], uids[0], outOfSpace, err = col.InsertWithUID(jsonDocs[0])
	if err != nil || outOfSpace {
		t.Fatal("insert error")
	}
	ids[1], uids[1], outOfSpace, err = col.InsertWithUID(jsonDocs[1])
	if err != nil || outOfSpace {
		t.Fatal("insert error")
	}
	ids[2], uids[2], outOfSpace, err = col.InsertWithUID(jsonDocs[2])
	if err != nil || outOfSpace {
		t.Fatal("insert error")
	}
	if len(uids[0]) != 32 || len(uids[1]) != 32 || len(uids[2]) != 32 ||
		uids[0] == uids[1] || uids[1] == uids[2] || uids[2] == uids[0] ||
		ids[0] == ids[1] || ids[1] == ids[2] || ids[2] == ids[0] {
		t.Fatalf("Malformed UIDs or IDs: %v %v", uids, ids)
	}
	// read - inexisting UID
	var readDoc interface{}
	if _, readErr := col.ReadByUID("abcde", &readDoc); readErr == nil {
		t.Fatal("It should have triggered UID not found error")
	}
	// read - existing UID
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
	if _, outOfSpace, err := col.UpdateByUID(uids[0], docWithoutUID); err != nil || outOfSpace { // intentionally remove UID
		t.Fatal(err)
	}
	if _, err = col.ReadByUID(uids[0], &readDoc); err == nil { // UID was removed therefore the UID is not found
		t.Fatalf("UpdateByUID did not work, still read %v", readDoc)
	}
	// update (reassign UID)
	newID, newUID, newDoc, outOfSpace, err := col.ReassignUID(ids[0])
	if newID != ids[0] || len(newUID) != 32 || err != nil || outOfSpace {
		t.Fatalf("ReassignUID did not work: %v %v %v", newID, newUID, err)
	}
	newDocMap, _ := newDoc.(map[string]interface{})
	if fmt.Sprint(newDocMap[UID_PATH]) != newUID {
		t.Fatalf("Reassign UID did not return correct new document")
	}
	// after UID reassignment, the old UID should be gone
	if _, readErr := col.ReadByUID(uids[0], &readDoc); readErr == nil {
		t.Fatal("It should have triggered UID not found error")
	}
	if _, err = col.ReadByUID(newUID, &readDoc); err != nil { // UID was reassigned, the error should NOT happen
		t.Fatalf("ReassignUID did not work")
	}
	// delete
	col.DeleteByUID(newUID)
	if _, err = col.ReadByUID(newUID, &readDoc); err == nil {
		t.Fatalf("DeleteByUID did not work")
	}
	col.Close()
	// Reopen and test read again
	reopen, err := OpenChunk(0, tmp)
	if err != nil {
		t.Fatal(err)
	}
	if !(len(col.Hashtables) == 1 && len(col.HTPaths) == 1 && len(col.Path2HT) == 1) {
		t.Fatal("Did not reopen UID index")
	}
	// UID index should work
	if _, err = reopen.ReadByUID(uids[1], &readDoc); err != nil {
		t.Fatalf("Reopen failed UID index?")
	}
	reopen.Close()
}

func TestOutOfSpace(t *testing.T) {
	tmp := "/tmp/tiedot_col_test"
	os.RemoveAll(tmp)
	defer os.RemoveAll(tmp)
	col, err := OpenChunk(0, tmp)
	if err != nil {
		t.Fatalf("Failed to open: %v", err)
		return
	}
	defer col.Close()
	var outOfSpace bool
	// Prepare a very long document - a single one will fill up 2/5 of a chunk
	longDocStr := `{"a": "` + strings.Repeat(" ", int(chunkfile.DOC_MAX_ROOM/5)) + `"}`
	var longDoc interface{}
	json.Unmarshal([]byte(longDocStr), &longDoc)
	_, _, outOfSpace, err = col.InsertWithUID(longDoc)
	if err != nil || outOfSpace {
		t.Fatalf("Failed to insert a long document: %v %v", err, outOfSpace)
	}
	_, docUID, outOfSpace, err := col.InsertWithUID(longDoc)
	if err != nil || outOfSpace {
		t.Fatalf("Failed to insert a long document: %v %v", err, outOfSpace)
	}
	// It should run out of space this time
	_, _, outOfSpace, err = col.InsertWithUID(longDoc)
	if err != nil || !outOfSpace {
		t.Fatalf("It should run out of space")
	}
	// Prepare a even longer document to overwrite the previous one (to trigger re-insert)
	longerDocStr := `{"a": "` + strings.Repeat(" ", int(chunkfile.DOC_MAX_ROOM/2-100)) + `"}`
	var longerDoc interface{}
	json.Unmarshal([]byte(longerDocStr), &longerDoc)
	_, outOfSpace, err = col.UpdateByUID(docUID, longerDoc)
	if !outOfSpace || err != nil {
		t.Fatalf("It Should run out of space %v %v", err, outOfSpace)
	}
}

func TestScrubAndColScan(t *testing.T) {
	tmp := "/tmp/tiedot_col_test"
	os.RemoveAll(tmp)
	defer os.RemoveAll(tmp)
	col, err := OpenChunk(0, tmp)
	if err != nil {
		t.Fatalf("Failed to open: %v", err)
		return
	}
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
	var doc interface{}
	json.Unmarshal([]byte(`{"a": [{"b": {"c": [4]}}, {"b": {"c": [5, 6]}}], "d": [0, 9]}`), &doc)
	for i := 0; i < 10000; i++ {
		_, outOfSpace, err := col.Insert(doc)
		if outOfSpace || err != nil {
			t.Fatal("Insert fault")
		}
	}
	// Do some serious damage to index and collection data
	for i := 0; i < 1024*1024*1; i++ {
		col.Hashtables[0].File.Buf[i] = 6
	}
	for i := 0; i < 1024*1024*1; i++ {
		col.Hashtables[1].File.Buf[i] = 6
	}
	for i := 1024 * 1024 * 1; i < 1024*1024*2; i++ {
		col.Hashtables[2].File.Buf[i] = 6
	}
	for i := 1024; i < 1024*128; i++ {
		col.Data.File.Buf[i] = 6
	}
	for i := 1024 * 256; i < 1024*512; i++ {
		col.Data.File.Buf[i] = 6
	}
	col.Close()
	// Reopen the chunk and expect data structure failure messages from log
	fmt.Println("Please ignore the following error messages")
	reopen, err := OpenChunk(0, tmp)
	recoveredNum := reopen.Scrub()
	// Confirm that 6528 documents are successfully recovered in four ways
	counter := 0
	// first - deserialization & scan
	var recoveredDoc interface{}
	reopen.DeserializeAll(&recoveredDoc, func(id uint64) bool {
		counter++
		return true
	})
	if counter != 6528 {
		t.Fatal("Did not recover enough documents")
	}
	// second - collection scan
	counter = 0
	reopen.ForAll(func(id uint64, doc interface{}) bool {
		counter++
		return true
	})
	if counter != 6528 {
		t.Fatal("Did not recover enough documents")
	}
	// third - index scan
	keys, vals := reopen.Hashtables[1].GetAll(0)
	if !(len(keys) == 6528*3 && len(vals) == 6528*3) {
		t.Fatalf("Did not recover enough documents on index, got only %d", len(vals))
	}
	// fourth - scrub return value
	if recoveredNum != 6528 {
		t.Fatal("Scrub return value is wrong")
	}
}
