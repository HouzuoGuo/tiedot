package data

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/HouzuoGuo/tiedot/dberr"
	"github.com/bouk/monkey"
)

// helper function
func RandStringBytes(n int) string {
	const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
}

// setUp
func setupTestCollection() (col *Collection, err error) {
	os.Remove(tmp)
	defer os.Remove(tmp)
	d := defaultConfig()
	return d.OpenCollection(tmp)
}

func TestInsertRead(t *testing.T) {
	os.Remove(tmp)
	defer os.Remove(tmp)
	data := defaultConfig()
	col, err := data.OpenCollection(tmp)
	if err != nil {
		t.Fatalf("Failed to open: %v", err)
		return
	}
	defer col.Close()
	docs := [][]byte{
		[]byte("abc"),
		[]byte("1234")}
	ids := [2]int{}
	if ids[0], err = col.Insert(docs[0]); ids[0] != 0 || err != nil {
		t.Fatalf("Failed to insert: %d %v", ids[0], err)
	}
	if ids[1], err = col.Insert(docs[1]); err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}
	if doc0 := col.Read(ids[0]); doc0 == nil || strings.TrimSpace(string(doc0)) != string(docs[0]) {
		t.Fatal("Failed to read", doc0)
	}
	if doc1 := col.Read(ids[1]); doc1 == nil || strings.TrimSpace(string(doc1)) != string(docs[1]) {
		t.Fatalf("Failed to read")
	}

	patch := monkey.Patch(binary.Varint, func(buf []byte) (int64, int) {
		return int64(col.DocMaxRoom + 1), 0
	})

	if doc1 := col.Read(ids[1]); doc1 != nil {
		t.Fatalf("Expected nill return if the buffer exceeds")
	}
	patch.Unpatch()

	patch2 := monkey.Patch(binary.Varint, func(buf []byte) (int64, int) {
		return int64(col.DocMaxRoom), 0
	})
	col.Size = col.DocMaxRoom
	if doc1 := col.Read(ids[1]); doc1 != nil {
		t.Fatalf("Expected nill return if doc eof")
	}
	patch2.Unpatch()

	// it shall not panic
	col.Read(col.Size)

	zero, err := col.Insert([]byte(RandStringBytes(col.DocMaxRoom)))

	if zero != 0 || err.Error() != "Document is too large. Max: `2097152`, Given: `4194304`" {
		t.Fatalf("Expected error document to large.")
	}

	var d *DataFile
	errEnsureSize := "test error from function EnsureSize"
	patch3 := monkey.PatchInstanceMethod(reflect.TypeOf(d), "EnsureSize", func(_ *DataFile, more int) (err error) {
		return errors.New(errEnsureSize)
	})

	if _, err := col.Insert(docs[0]); err.Error() != errEnsureSize {
		t.Fatalf("Expected err from function EnsureSize")
	}
	patch3.Unpatch()
}
func TestInsertUpdateRead(t *testing.T) {
	os.Remove(tmp)
	defer os.Remove(tmp)
	d := defaultConfig()
	col, err := d.OpenCollection(tmp)
	if err != nil {
		t.Fatalf("Failed to open: %v", err)
		return
	}
	defer col.Close()
	docs := [][]byte{
		[]byte("abc"),
		[]byte("1234")}
	ids := [2]int{}
	if ids[0], err = col.Insert(docs[0]); err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}
	if ids[1], err = col.Insert(docs[1]); err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}
	updated := [2]int{}
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
	col.Update(col.Size, []byte("abcdef"))
}
func TestEndDocumentExceeded(t *testing.T) {
	var id int
	os.Remove(tmp)
	defer os.Remove(tmp)
	d := defaultConfig()
	col, err := d.OpenCollection(tmp)
	defer col.Close()

	if id, err = col.Insert([]byte("test")); err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}
	patch := monkey.Patch(binary.Varint, func(buf []byte) (int64, int) {
		return int64(col.DocMaxRoom), 0
	})
	col.Size = 0

	if _, err = col.Update(id, []byte("")); err != nil && err.Error() != dberr.New(dberr.ErrorNoDoc, id).Error() {
		t.Fatalf("Expected return document does not exist")
	}
	patch.Unpatch()
}

func TestVariantRetuneMoreMaxDocument(t *testing.T) {
	var id int
	col, err := setupTestCollection()
	defer col.Close()
	if id, err = col.Insert([]byte("test")); err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	patch := monkey.Patch(binary.Varint, func(buf []byte) (int64, int) {
		return int64(col.DocMaxRoom + 1), 0
	})
	if _, err = col.Update(id, []byte("")); err != nil && err.Error() != dberr.New(dberr.ErrorNoDoc, id).Error() {
		t.Fatalf("Expected return document does not exist")
	}
	patch.Unpatch()
}
func TestUpdateMoreThanMaxDocument(t *testing.T) {
	col, err := setupTestCollection()
	defer col.Close()
	if err != nil {
		t.Fatalf("Failed to open: %v", err)
		return
	}
	if _, err = col.Update(10, []byte(RandStringBytes(col.DocMaxRoom+1))); err != nil && err.Error() != fmt.Sprintf("Document is too large. Max: `%d`, Given: `%d`", col.DocMaxRoom, col.DocMaxRoom+1) {
		t.Fatal("Expected error document is too large")
	}
}
func TestUpdate(t *testing.T) {
	var id int
	col, err := setupTestCollection()
	defer col.Close()
	if id, err = col.Insert([]byte("test")); err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	patch := monkey.Patch(binary.Varint, func(buf []byte) (int64, int) {
		return int64(160), 0
	})
	if _, err = col.Update(id, []byte("test")); err != nil && err.Error() != dberr.New(dberr.ErrorNoDoc, id).Error() {
		t.Fatalf("Expected return document does not exist")
	}
	patch.Unpatch()
}
func TestInsertDeleteRead(t *testing.T) {
	os.Remove(tmp)
	defer os.Remove(tmp)
	d := defaultConfig()
	col, err := d.OpenCollection(tmp)
	if err != nil {
		t.Fatalf("Failed to open: %v", err)
		return
	}
	defer col.Close()
	docs := [][]byte{
		[]byte("abc"),
		[]byte("1234"),
		[]byte("2345")}
	ids := [3]int{}
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
	if err = col.Delete(ids[1]); err != nil {
		t.Fatal(err)
	}
	if doc1 := col.Read(ids[1]); doc1 != nil {
		t.Fatalf("Did not delete")
	}
	if doc2 := col.Read(ids[2]); doc2 == nil || strings.TrimSpace(string(doc2)) != string(docs[2]) {
		t.Fatalf("Failed to read")
	}
	// it shall not panic
	if err = col.Delete(col.Size); dberr.Type(err) != dberr.ErrorNoDoc {
		t.Fatal("did not error")
	}
}
func TestInsertReadAll(t *testing.T) {
	os.Remove(tmp)
	defer os.Remove(tmp)
	d := defaultConfig()
	col, err := d.OpenCollection(tmp)
	if err != nil {
		t.Fatalf("Failed to open: %v", err)
		return
	}
	defer col.Close()
	var ids [5]int
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
	successfullyRead := 0
	t.Log(ids)
	col.ForEachDoc(func(_ int, _ []byte) bool {
		successfullyRead++
		return true
	})
	// break
	col.ForEachDoc(func(_ int, _ []byte) bool {
		return false
	})
	if successfullyRead != 5 {
		t.Fatalf("Should have read 5 documents, but only got %d", successfullyRead)
	}
	successfullyRead = 0
	// intentionally corrupt two docuemnts
	col.Buf[ids[4]] = 3     // corrupted validity
	col.Buf[ids[2]+1] = 255 // corrupted room
	col.ForEachDoc(func(_ int, _ []byte) bool {
		successfullyRead++
		return true
	})
	if successfullyRead != 3 {
		t.Fatalf("Should have read 3 documents, but %d are recovered", successfullyRead)
	}
}

func TestCollectionGrowAndOutOfBoundAccess(t *testing.T) {
	os.Remove(tmp)
	defer os.Remove(tmp)
	d := defaultConfig()
	col, err := d.OpenCollection(tmp)
	if err != nil {
		t.Fatalf("Failed to open: %v", err)
		return
	}
	defer col.Close()
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
	calculatedUsedSize := (DocHeader + 3*2) + (DocHeader+4*2)*2
	if col.Used != calculatedUsedSize {
		t.Fatalf("Invalid UsedSize")
	}
	// Read invalid location
	if doc := col.Read(1); doc != nil {
		t.Fatalf("Read invalid location")
	}
	if doc := col.Read(col.Used); doc != nil {
		t.Fatalf("Read invalid location")
	}
	if doc := col.Read(col.Size); doc != nil {
		t.Fatalf("Read invalid location")
	}
	if doc := col.Read(999999999); doc != nil {
		t.Fatalf("Read invalid location")
	}
	// Update invalid location
	if _, err := col.Update(1, []byte{}); dberr.Type(err) != dberr.ErrorNoDoc {
		t.Fatalf("Update invalid location")
	}
	if _, err := col.Update(col.Used, []byte{}); dberr.Type(err) != dberr.ErrorNoDoc {
		t.Fatalf("Update invalid location")
	}
	if _, err := col.Update(col.Size, []byte{}); dberr.Type(err) != dberr.ErrorNoDoc {
		t.Fatalf("Update invalid location")
	}
	if _, err := col.Update(999999999, []byte{}); dberr.Type(err) != dberr.ErrorNoDoc {
		t.Fatalf("Update invalid location")
	}
	// Delete invalid location
	if err = col.Delete(1); dberr.Type(err) != dberr.ErrorNoDoc {
		t.Fatal("did not error")
	}
	if err = col.Delete(col.Used); dberr.Type(err) != dberr.ErrorNoDoc {
		t.Fatal("did not error")
	}
	if err = col.Delete(col.Size); dberr.Type(err) != dberr.ErrorNoDoc {
		t.Fatal("did not error")
	}
	if err = col.Delete(999999999); dberr.Type(err) != dberr.ErrorNoDoc {
		t.Fatal("did not error")
	}
	// Insert - not enough room
	count := 0
	for i := 0; i < col.ColFileGrowth; i += col.DocMaxRoom {
		if _, err := col.Insert(make([]byte, col.DocMaxRoom/2)); err != nil {
			t.Fatal(err)
		}
		count++
	}
	if _, err := col.Insert(make([]byte, col.DocMaxRoom/2)); err != nil {
		t.Fatal(err)
	}
	count++
	calculatedUsedSize += count * (DocHeader + col.DocMaxRoom)
	if col.Used != calculatedUsedSize {
		t.Fatalf("Wrong UsedSize %d %d", col.Used, calculatedUsedSize)
	}
	if col.Size != col.ColFileGrowth+col.Growth {
		t.Fatalf("Size changed?! %d %d %d", col.Size, col.ColFileGrowth, col.Growth)
	}
	if err = col.Close(); err != nil {
		t.Fatal(err)
	}
}
