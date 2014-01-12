package db

import (
	"encoding/json"
	"fmt"
	"github.com/HouzuoGuo/tiedot/chunk"
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

func TestDocCRUD(t *testing.T) {
	fmt.Println("Running CRUD test")
	tmp := "/tmp/tiedot_col_test"
	os.RemoveAll(tmp)
	defer os.RemoveAll(tmp)
	col, err := OpenCol(tmp, 4)
	if err != nil {
		t.Fatalf("Failed to open: %v", err)
		return
	}
	docs := []string{
		`{"a": {"b": {"c": 1}}, "d": 1, "more": "` + strings.Repeat(" ", int(chunkfile.DOC_MAX_ROOM/3)) + `"}`,
		`{"a": {"b": {"c": 2}}, "d": 2, "more": "` + strings.Repeat(" ", int(chunkfile.DOC_MAX_ROOM/3)) + `"}`,
		`{"a": {"b": {"c": 3}}, "d": 3, "more": "` + strings.Repeat(" ", int(chunkfile.DOC_MAX_ROOM/3)) + `"}`}
	var jsonDocs [3]map[string]interface{}
	var uids [3]string
	json.Unmarshal([]byte(docs[0]), &jsonDocs[0])
	json.Unmarshal([]byte(docs[1]), &jsonDocs[1])
	json.Unmarshal([]byte(docs[2]), &jsonDocs[2])
	// insert
	uids[0], err = col.Insert(jsonDocs[0])
	if err != nil {
		t.Fatal("insert error")
	}
	uids[1], err = col.Insert(jsonDocs[1])
	if err != nil {
		t.Fatal("insert error")
	}
	uids[2], err = col.Insert(jsonDocs[2])
	if err != nil {
		t.Fatal("insert error")
	}
	if len(uids[0]) != 32 || len(uids[1]) != 32 || len(uids[2]) != 32 ||
		uids[0] == uids[1] || uids[1] == uids[2] || uids[2] == uids[0] {
		t.Fatalf("Malformed UIDs or IDs: %v %v", uids)
	}
	// read - non-existing UID
	var readDoc map[string]interface{}
	if _, readErr := col.Read("abcde", &readDoc); readErr == nil {
		t.Fatal("It should have triggered UID not found error")
	}
	// read - existing UID
	_, readErr := col.Read(uids[1], &readDoc)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if fmt.Sprint(readDoc["a"]) != fmt.Sprint(jsonDocs[1]["a"]) {
		t.Fatalf("Cannot read back original document by ID")
	}
	// update
	var docWithoutUID map[string]interface{}
	json.Unmarshal([]byte(docs[1]), &docWithoutUID)
	if err := col.Update(uids[0], docWithoutUID); err != nil { // intentionally remove UID
		t.Fatal(err)
	}
	if _, err = col.Read(uids[0], &readDoc); err != nil { // ID shall reappear
		t.Fatalf("Document went missing after update: %v", err)
	}
	// delete
	col.Delete(uids[0])
	if _, err = col.Read(uids[0], &readDoc); err == nil {
		t.Fatalf("DeleteByUID did not work")
	}
	if _, err = col.Read(uids[1], &readDoc); err != nil {
		t.Fatalf("col failed UID index? %s %v", uids[1], err)
	}
	col.Close()
	// Reopen and test read again
	reopen, err := OpenCol(tmp, 4)
	if err != nil {
		t.Fatal(err)
	}
	// UID index should work
	if _, err = reopen.Read(uids[1], &readDoc); err != nil {
		t.Fatalf("Reopen failed UID index? %s %v", uids[1], err)
	}
	// Scrub the entire collection, number of chunks should remain
	//	if reopen.Scrub() != 2 {
	//		t.Fatal("Scrub recovered wrong number of documents")
	//	}
	if reopen.NumChunks != 4 {
		t.Fatal("Scrub caused chunk number change")
	}
	if err = reopen.Flush(); err != nil {
		t.Fatal(err)
	}
	reopen.Close()
}

func SecIndexContainsAll(path string, col *Col, expectedKV map[uint64][]string) bool {
	for k, ids := range expectedKV {
		fmt.Printf("Looking for key %v, id %v\n", k, ids)
		keys, vals := col.HashScan(path, k, 0, func(_, _ uint64) bool {
			return true
		})
		if len(keys) == 0 || len(vals) == 0 {
			fmt.Printf("Hash table does not have the key\n")
			return false
		}
		if len(vals) != len(ids) {
			fmt.Printf("Number not matched: %v %v\n", vals, ids)
			return false
		}
		for _, id := range ids {
			fmt.Printf("Checking for ID %s match among physical IDs %v\n", id, vals)
			var doc interface{}
			_, err := col.Read(id, &doc)
			if err != nil {
				fmt.Printf("ID given by function parameter does not exist %s\n", id)
				panic(err)
			}
			match := false
			for _, v := range vals {
				if chunk.StrHash(id) == v {
					match = true
					break
				}
			}
			if !match {
				fmt.Printf("Hash table value does not match with ID hash %v %v\n", chunk.StrHash(id), vals[0])
				return false
			}
		}
	}
	return true
}

func TestIndex(t *testing.T) {
	fmt.Println("Running index test")
	tmp := "/tmp/tiedot_col_test"
	os.RemoveAll(tmp)
	defer os.RemoveAll(tmp)
	col, err := OpenCol(tmp, 4)
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
	var jsonDoc [4]map[string]interface{}
	json.Unmarshal([]byte(docs[0]), &jsonDoc[0])
	json.Unmarshal([]byte(docs[1]), &jsonDoc[1])
	json.Unmarshal([]byte(docs[2]), &jsonDoc[2])
	json.Unmarshal([]byte(docs[3]), &jsonDoc[3])
	var ids [4]string
	// Insert a document, create two indexes and verify them
	ids[0], _ = col.Insert(jsonDoc[0])
	col.Index([]string{"a", "b", "c"})
	col.Index([]string{"d"})
	if !SecIndexContainsAll("a,b,c", col, map[uint64][]string{chunk.StrHash("1"): []string{ids[0]}}) {
		t.Fatal()
	}
	if !SecIndexContainsAll("a,b,c", col, map[uint64][]string{chunk.StrHash("1"): []string{ids[0]}}) {
		t.Fatal()
	}
	// Do the following:
	// 1. Insert second and third document
	// 2. Replace the third document by the fourth document
	// 3. Remove the second document
	ids[1], _ = col.Insert(jsonDoc[1])
	ids[2], _ = col.Insert(jsonDoc[2])
	col.Update(ids[2], jsonDoc[3])
	col.Delete(ids[1])
	// Now the first and fourth documents are left, scrub and reopen the collection and verify index
	//	col.Scrub()
	col.Close()
	col, err = OpenCol(tmp, 4)
	if err != nil {
		t.Fatalf("Failed to reopen: %v", err)
	}
	if !SecIndexContainsAll("d", col, map[uint64][]string{chunk.StrHash("0"): []string{ids[0], ids[2]}}) {
		t.Fatal()
	}
	if !SecIndexContainsAll("a,b,c", col, map[uint64][]string{chunk.StrHash("1"): []string{ids[0]}}) {
		t.Fatal()
	}
	if !SecIndexContainsAll("a,b,c", col, map[uint64][]string{chunk.StrHash("4"): []string{ids[2]}}) {
		t.Fatal()
	}
	// Insert one more document and verify indexes
	newID, _ := col.Insert(jsonDoc[0])
	if !SecIndexContainsAll("d", col, map[uint64][]string{chunk.StrHash("0"): []string{ids[0], ids[2], newID}}) {
		t.Fatal()
	}
	if !SecIndexContainsAll("a,b,c", col, map[uint64][]string{chunk.StrHash("1"): []string{ids[0], newID}}) {
		t.Fatal()
	}
	if !SecIndexContainsAll("a,b,c", col, map[uint64][]string{chunk.StrHash("4"): []string{ids[2]}}) {
		t.Fatal()
	}
	if err = col.Flush(); err != nil {
		t.Fatal(err)
	}
	col.Close()
}
