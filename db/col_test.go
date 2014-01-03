/* Collection chunks coordination test. */
package db

import (
	"encoding/json"
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
