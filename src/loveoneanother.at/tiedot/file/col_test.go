package file

import (
	"os"
	"strings"
	"testing"
)

func TestInsertRead(t *testing.T) {
	tmp := "/tmp/tiedot_col_test"
	defer os.Remove(tmp)
	col, err := OpenCol(tmp)
	if err != nil {
		t.Errorf("Failed to open: %v", err)
	}
	docs := [][]byte{
		[]byte("abc"),
		[]byte("1234")}
	ids := [2]uint64{}
	if ids[0], err = col.Insert(docs[0]); err != nil {
		t.Errorf("Failed to insert: %v", err)
	}
	if ids[1], err = col.Insert(docs[1]); err != nil {
		t.Errorf("Failed to insert: %v", err)
	}
	if doc0, err := col.Read(ids[0]); err != nil || strings.Trim(string(doc0), "\000") != string(docs[0]) {
		t.Errorf("Failed to read: %v", err)
	}
	if doc1, err := col.Read(ids[1]); err != nil || strings.Trim(string(doc1), "\000") != string(docs[1]) {
		t.Errorf("Failed to read: %v", err)
	}
	col.File.Close()
}

func TestInsertUpdateRead(t *testing.T) {
	tmp := "/tmp/tiedot_col_test"
	defer os.Remove(tmp)
	col, err := OpenCol(tmp)
	if err != nil {
		t.Errorf("Failed to open: %v", err)
	}
	docs := [][]byte{
		[]byte("abc"),
		[]byte("1234")}
	ids := [2]uint64{}
	if ids[0], err = col.Insert(docs[0]); err != nil {
		t.Errorf("Failed to insert: %v", err)
	}
	if ids[1], err = col.Insert(docs[1]); err != nil {
		t.Errorf("Failed to insert: %v", err)
	}
	updated := [2]uint64{}
	if updated[0], err = col.Update(ids[0], []byte("bcd")); err != nil || updated[0] != ids[0] {
		t.Errorf("Failed to update: %v", err)
	}
	if updated[1], err = col.Update(ids[1], []byte("longlonglong")); err != nil || updated[1] == ids[1] {
		t.Errorf("Failed to update: %v", err)
	}
	if doc0, err := col.Read(updated[0]); err != nil || strings.Trim(string(doc0), "\000") != "bcd" {
		t.Errorf("Failed to read: %v", err)
	}
	if doc1, err := col.Read(updated[1]); err != nil || strings.Trim(string(doc1), "\000") != "longlonglong" {
		t.Errorf("Failed to read: %v", err)
	}
	col.File.Close()
}

func TestInsertDeleteRead(t *testing.T) {
	tmp := "/tmp/tiedot_col_test"
	defer os.Remove(tmp)
	col, err := OpenCol(tmp)
	if err != nil {
		t.Errorf("Failed to open: %v", err)
	}
	docs := [][]byte{
		[]byte("abc"),
		[]byte("1234"),
		[]byte("2345")}
	ids := [3]uint64{}
	if ids[0], err = col.Insert(docs[0]); err != nil {
		t.Errorf("Failed to insert: %v", err)
	}
	if ids[1], err = col.Insert(docs[1]); err != nil {
		t.Errorf("Failed to insert: %v", err)
	}
	if ids[2], err = col.Insert(docs[2]); err != nil {
		t.Errorf("Failed to insert: %v", err)
	}
	if doc0, err := col.Read(ids[0]); err != nil || strings.Trim(string(doc0), "\000") != string(docs[0]) {
		t.Errorf("Failed to read: %v", err)
	}
	col.Delete(ids[1])
	if doc1, err := col.Read(ids[1]); err != nil || doc1 != nil {
		t.Errorf("Did not delete")
	}
	if doc2, err := col.Read(ids[2]); err != nil || strings.Trim(string(doc2), "\000") != string(docs[2]) {
		t.Errorf("Failed to read: %v", err)
	}
	col.File.Close()
}
