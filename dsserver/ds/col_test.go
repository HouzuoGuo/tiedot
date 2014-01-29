/* Document collection file test. */
package ds

import (
	"fmt"
	"os"
	"strings"
	"testing"
)

func TestInsertRead(t *testing.T) {
	tmp := "/tmp/tiedot_col_test"
	os.Remove(tmp)
	defer os.Remove(tmp)
	col, err := OpenCol(tmp)
	if err != nil {
		t.Fatalf("Failed to open: %v", err)
		return
	}
	defer col.File.Close()
	docs := [][]byte{
		[]byte("abc"),
		[]byte("1234")}
	ids := [2]uint64{}
	if ids[0], err = col.Insert(docs[0]); err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}
	if ids[1], err = col.Insert(docs[1]); err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}
	if doc0 := col.Read(ids[0]); doc0 == nil || strings.TrimSpace(string(doc0)) != string(docs[0]) {
		t.Fatalf("Failed to read")
	}
	if doc1 := col.Read(ids[1]); doc1 == nil || strings.TrimSpace(string(doc1)) != string(docs[1]) {
		t.Fatalf("Failed to read")
	}
	// it shall not panic
	col.Read(col.File.Size)
}

func TestInsertReadAll(t *testing.T) {
	tmp := "/tmp/tiedot_col_test"
	os.Remove(tmp)
	defer os.Remove(tmp)
	col, err := OpenCol(tmp)
	if err != nil {
		t.Fatalf("Failed to open: %v", err)
		return
	}
	defer col.File.Close()
	var ids [5]uint64
	ids[0], err = col.Insert([]byte("abc"))
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}
	ids[1], err = col.Insert([]byte("abc"))
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}
	ids[2], err = col.Insert([]byte("abc"))
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}
	ids[3], err = col.Insert([]byte("abc"))
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}
	ids[4], err = col.Insert([]byte("abc"))
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}
	fmt.Println("Please ignore the following Corrupted Document error messages, they are intentional.")
	// intentionally corrupt two docuemnts
	col.File.Buf[ids[4]] = 3     // corrupted validity
	col.File.Buf[ids[1]+1] = 255 // corrupted room
	col.File.Buf[ids[1]+2] = 255
	col.File.Buf[ids[1]+3] = 255
	col.File.Buf[ids[1]+4] = 255
	successfullyRead := 0
	col.ForAll(func(id uint64, data []byte) bool {
		successfullyRead++
		return true
	})
	// the reason is that corrupted documents are "empty" documents
	if successfullyRead != 3 {
		t.Fatalf("Should have read 3 documents, but %d are recovered", successfullyRead)
	}
}

func TestInsertUpdateRead(t *testing.T) {
	tmp := "/tmp/tiedot_col_test"
	os.Remove(tmp)
	defer os.Remove(tmp)
	col, err := OpenCol(tmp)
	if err != nil {
		t.Fatalf("Failed to open: %v", err)
		return
	}
	defer col.File.Close()
	docs := [][]byte{
		[]byte("abc"),
		[]byte("1234")}
	ids := [2]uint64{}
	if ids[0], err = col.Insert(docs[0]); err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}
	if ids[1], err = col.Insert(docs[1]); err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}
	updated := [2]uint64{}
	if updated[0], err = col.Update(ids[0], []byte("abcdef")); err != nil || updated[0] != ids[0] {
		t.Fatalf("Failed to update: %v", err)
	}
	if updated[1], err = col.Update(ids[1], []byte("longlonglonglonglonglonglong")); err != nil || updated[1] == ids[1] {
		t.Fatalf("Failed to update: %v", err)
	}
	if doc0 := col.Read(updated[0]); doc0 == nil || strings.TrimSpace(string(doc0)) != "abcdef" {
		t.Fatalf("Failed to read")
	}
	if doc1 := col.Read(updated[1]); doc1 == nil || strings.TrimSpace(string(doc1)) != "longlonglonglonglonglonglong" {
		t.Fatalf("Failed to read")
	}
	// it shall not panic
	col.Update(col.File.Size, []byte("abcdef"))
}

func TestInsertDeleteRead(t *testing.T) {
	tmp := "/tmp/tiedot_col_test"
	os.Remove(tmp)
	defer os.Remove(tmp)
	col, err := OpenCol(tmp)
	if err != nil {
		t.Fatalf("Failed to open: %v", err)
		return
	}
	defer col.File.Close()
	docs := [][]byte{
		[]byte("abc"),
		[]byte("1234"),
		[]byte("2345")}
	ids := [3]uint64{}
	if ids[0], err = col.Insert(docs[0]); err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}
	if ids[1], err = col.Insert(docs[1]); err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}
	if ids[2], err = col.Insert(docs[2]); err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}
	if doc0 := col.Read(ids[0]); doc0 == nil || strings.TrimSpace(string(doc0)) != string(docs[0]) {
		t.Fatalf("Failed to read")
	}
	col.Delete(ids[1])
	if doc1 := col.Read(ids[1]); doc1 != nil {
		t.Fatalf("Did not delete")
	}
	if doc2 := col.Read(ids[2]); doc2 == nil || strings.TrimSpace(string(doc2)) != string(docs[2]) {
		t.Fatalf("Failed to read")
	}
	// it shall not panic
	col.Delete(col.File.Size)
}

func TestFileGrowAndOutOfBoundAccess(t *testing.T) {
	tmp := "/tmp/tiedot_col_test"
	os.Remove(tmp)
	defer os.Remove(tmp)
	col, err := OpenCol(tmp)
	if err != nil {
		t.Fatalf("Failed to open: %v", err)
		return
	}
	defer col.File.Close()
	// Insert several documents
	docs := [][]byte{
		[]byte("abc"),
		[]byte("1234"),
		[]byte("2345")}
	if _, err = col.Insert(docs[0]); err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}
	if _, err = col.Insert(docs[1]); err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}
	if _, err = col.Insert(docs[2]); err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}
	// Test UsedSize
	calculatedUsedSize := uint64((DOC_HEADER_SIZE + 3*2) + (DOC_HEADER_SIZE+4*2)*2)
	if col.File.UsedSize != calculatedUsedSize {
		t.Fatalf("Invalid UsedSize")
	}
	// Read invalid location
	if doc := col.Read(1); doc != nil {
		t.Fatalf("Read invalid location")
	}
	if doc := col.Read(col.File.UsedSize); doc != nil {
		t.Fatalf("Read invalid location")
	}
	if doc := col.Read(col.File.Size); doc != nil {
		t.Fatalf("Read invalid location")
	}
	if doc := col.Read(999999999); doc != nil {
		t.Fatalf("Read invalid location")
	}
	// Update invalid location
	if _, err := col.Update(1, []byte{}); err == nil {
		t.Fatalf("Update invalid location")
	}
	if _, err := col.Update(col.File.UsedSize, []byte{}); err == nil {
		t.Fatalf("Update invalid location")
	}
	if _, err := col.Update(col.File.Size, []byte{}); err == nil {
		t.Fatalf("Update invalid location")
	}
	if _, err := col.Update(999999999, []byte{}); err == nil {
		t.Fatalf("Update invalid location")
	}
	// Delete invalid location
	col.Delete(1)
	col.Delete(col.File.UsedSize)
	col.Delete(col.File.Size)
	col.Delete(999999999)
	// Insert - not enough room (assuming COL_FILE_SIZE == DOC_MAX_ROOM)
	if _, err := col.Insert(make([]byte, DOC_MAX_ROOM/2)); err != nil {
		panic(err)
	}
	calculatedUsedSize += DOC_HEADER_SIZE + DOC_MAX_ROOM
	if col.File.UsedSize != calculatedUsedSize {
		t.Fatalf("Wrong UsedSize")
	}
	if col.File.Size != COL_FILE_SIZE+col.File.Growth {
		t.Fatalf("Size changed?!")
	}
	// Update - not enough room (assuming COL_FILE_SIZE == DOC_MAX_ROOM)
	if _, err := col.Update(0, make([]byte, DOC_MAX_ROOM/2)); err != nil {
		panic(err)
	}
	calculatedUsedSize += DOC_HEADER_SIZE + DOC_MAX_ROOM
	if col.File.UsedSize != calculatedUsedSize {
		t.Fatalf("Wrong UsedSize")
	}
	if col.File.Size != COL_FILE_SIZE+col.File.Growth+col.File.Growth {
		t.Fatalf("Size changed?!")
	}
}
