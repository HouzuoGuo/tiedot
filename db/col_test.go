/* Collection chunks coordination test. */
package db

import (
	"encoding/json"
	"fmt"
	"github.com/HouzuoGuo/tiedot/chunkfile"
	"os"
	"strconv"
	"strings"
	"testing"
)

func TestDocIndexAndCRUD(t *testing.T) {
	tmp := "/tmp/tiedot_col_test"
	os.RemoveAll(tmp)
	defer os.RemoveAll(tmp)
	col, err := OpenCol(tmp)
	if err != nil {
		t.Fatal(err)
	}
	// Create an index
	if err = col.Index([]string{"id"}); err != nil {
		t.Fatal(err)
	}
	// Insert 10 long documents, afterwards there should be 5 chunks
	longDocIDs := make([]uint64, 10)
	for i := 0; i < 10; i++ {
		var longDoc interface{}
		json.Unmarshal([]byte(`{"a": "`+strings.Repeat("1", int(chunkfile.DOC_MAX_ROOM/6))+`", "id": `+strconv.Itoa(i)+`}`), &longDoc)
		if longDocIDs[i], err = col.Insert(longDoc); err != nil {
			t.Fatal(err)
		}
	}
	if !(len(col.ChunkMutexes) == 5 && len(col.Chunks) == 5 && col.NumChunks == 5) {
		t.Fatalf("Wrong number of chunks, got %d instead", col.NumChunks)
	}
	// There should still be enough room for 10 very short documents - even if they all go into one chunk
	shortDocIDs := make([]uint64, 10)
	for i := 0; i < 10; i++ {
		var shortDoc interface{}
		json.Unmarshal([]byte(`{"id": `+strconv.Itoa(i)+`}`), &shortDoc)
		if shortDocIDs[i], err = col.Insert(shortDoc); err != nil {
			t.Fatal(err)
		}
	}
	if !(len(col.ChunkMutexes) == 5 && len(col.Chunks) == 5 && col.NumChunks == 5) {
		t.Fatalf("There are too many chunks %d", col.NumChunks)
	}
	// Verify that docs can be read back using IDs
	matchIDAttr := func(doc interface{}, toMatch int) {
		docMap, _ := doc.(map[string]interface{})
		idAttr := docMap["id"]
		idAttrFloat, _ := idAttr.(float64)
		if int(idAttrFloat) != toMatch {
			t.Fatal("ID attribute mismatch")
		}
	}
	for i := 0; i < 10; i++ {
		var doc interface{}
		if err = col.Read(longDocIDs[i], &doc); err != nil {
			t.Fatal(err)
		}
		matchIDAttr(doc, i)
		if err = col.Read(shortDocIDs[i], &doc); err != nil {
			t.Fatal(err)
		}
		matchIDAttr(doc, i)
	}
	// Update a short document - no relocation
	var shortDoc interface{}
	err = json.Unmarshal([]byte(`{"id": 11}`), &shortDoc)
	if err != nil {
		t.Fatal(err)
	}
	shortNewID, err := col.Update(shortDocIDs[0], shortDoc)
	if err != nil {
		t.Fatal(err)
	} else if shortNewID != shortDocIDs[0] {
		t.Fatal("Doc was relocated, but it should not happen")
	}
	col.Read(shortNewID, &shortDoc)
	matchIDAttr(shortDoc, 11)
	// Reopen the collection
	col.Close()
	if col, err = OpenCol(tmp); err != nil {
		t.Fatal(err)
	}
	// Update a long document - relocation happens
	var longDoc interface{}
	json.Unmarshal([]byte(`{"a": "`+strings.Repeat("1", int(chunkfile.DOC_MAX_ROOM/3+2048))+`", "id": 11}`), &longDoc)
	longNewID, err := col.Update(longDocIDs[0], longDoc)
	if err != nil {
		t.Fatal(err)
	} else if longNewID == longDocIDs[0] {
		t.Fatal("Doc should be relocated, it did not happen")
	} else if col.NumChunks != 6 {
		// Relocated document has to go to a new chunk, therefore chunk number should be 6
		t.Fatal("Relocated doc should cause a new chunk to be created")
	}
	col.Read(longNewID, &longDoc)
	matchIDAttr(longDoc, 11)
	// Scrub the entire collection, number of chunks should remain
	if col.Scrub() != 20 {
		t.Fatal("Scrub recovered wrong number of documents")
	}
	if col.NumChunks != 6 {
		t.Fatal("Scrub caused chunk number change")
	}
	if err = col.Flush(); err != nil {
		t.Fatal(err)
	}
	// All chunks should have identical indexes
	for _, chunk := range col.Chunks {
		if _, ok := chunk.Path2HT["id"]; !ok {
			t.Fatal("Chunk does not have id index")
		}
	}
	// Delete a document
	col.Delete(longDocIDs[7])
	if err = col.Read(longDocIDs[7], &longDoc); err == nil {
		t.Fatal("Did not delete the document")
	}

	// Two collection scan
	counter := 0
	var template interface{}
	var throwAway interface{}
	col.DeserializeAll(&template, func(id uint64) bool {
		if err = col.ReadNoLock(id, &throwAway); err != nil {
			t.Fatal(err)
		}
		counter++
		return true
	})
	if counter != 19 { // 10 long docs, 10 short docs, one less long doc
		t.Fatal("Collection scan wrong number of docs")
	}
	counter = 0
	col.ForAll(func(id uint64, doc interface{}) bool {
		if err = col.ReadNoLock(id, &throwAway); err != nil {
			t.Fatal(err)
		}
		counter++
		return true
	})
	if counter != 19 {
		t.Fatal("Collection scan wrong number of docs")
	}
	// Remove index - it should be removed from all chunks
	if err = col.Unindex([]string{"id"}); err != nil {
		t.Fatal(err)
	}
	for _, chunk := range col.Chunks {
		if _, ok := chunk.Path2HT["id"]; ok {
			t.Fatal("Chunk should not have id index")
		}
	}
	// Out of bound access
	if err = col.Read(9999999, &longDoc); err == nil {
		t.Fatal("Out of bound access did not return error")
	}
	if _, err = col.Update(9999999, longDoc); err == nil {
		t.Fatal("Out of bound access did not return error")
	}
	col.Delete(99999999) // shall not crash
	col.Close()
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
	docs := []string{
		`{"a": {"b": {"c": 1}}, "d": 1, "more": "` + strings.Repeat(" ", int(chunkfile.DOC_MAX_ROOM/3)) + `"}`,
		`{"a": {"b": {"c": 2}}, "d": 2, "more": "` + strings.Repeat(" ", int(chunkfile.DOC_MAX_ROOM/3)) + `"}`,
		`{"a": {"b": {"c": 3}}, "d": 3, "more": "` + strings.Repeat(" ", int(chunkfile.DOC_MAX_ROOM/3)) + `"}`}
	var jsonDocs [3]interface{}
	var ids [3]uint64
	var uids [3]string
	json.Unmarshal([]byte(docs[0]), &jsonDocs[0])
	json.Unmarshal([]byte(docs[1]), &jsonDocs[1])
	json.Unmarshal([]byte(docs[2]), &jsonDocs[2])
	// insert
	ids[0], uids[0], err = col.InsertWithUID(jsonDocs[0])
	if err != nil {
		t.Fatal("insert error")
	}
	ids[1], uids[1], err = col.InsertWithUID(jsonDocs[1])
	if err != nil {
		t.Fatal("insert error")
	}
	ids[2], uids[2], err = col.InsertWithUID(jsonDocs[2])
	if err != nil {
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
		t.Fatalf("Cannot read back original document by UID: %d %d", readID, ids[1])
	}
	// update
	var docWithoutUID interface{}
	json.Unmarshal([]byte(docs[1]), &docWithoutUID)
	if _, err := col.UpdateByUID(uids[0], docWithoutUID); err != nil { // intentionally remove UID
		t.Fatal(err)
	}
	if _, err = col.ReadByUID(uids[0], &readDoc); err == nil { // UID was removed therefore the UID is not found
		t.Fatalf("UpdateByUID did not work, still read %v", readDoc)
	}
	// update (reassign UID)
	_, newUID, err := col.ReassignUID(ids[0])
	if len(newUID) != 32 || err != nil {
		t.Fatalf("ReassignUID did not work: %v %v %v", ids[0], newUID, err)
	}
	if _, err = col.ReadByUID(uids[1], &readDoc); err != nil {
		t.Fatalf("col failed UID index? %s %v", uids[1], err)
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
	if _, err = col.ReadByUID(uids[1], &readDoc); err != nil {
		t.Fatalf("col failed UID index? %s %v", uids[1], err)
	}
	col.Close()
	// Reopen and test read again
	reopen, err := OpenCol(tmp)
	if err != nil {
		t.Fatal(err)
	}
	// UID index should work
	if _, err = reopen.ReadByUID(uids[1], &readDoc); err != nil {
		t.Fatalf("Reopen failed UID index? %s %v", uids[1], err)
	}
	reopen.Close()
}
