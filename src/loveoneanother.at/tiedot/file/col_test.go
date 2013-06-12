package file

import (
	"math/rand"
	"os"
	"strings"
	"testing"
	"time"
)

const (
	COL_BENCH_SIZE = 1000000 // Number of documents made available for collection benchmark
)

func TestInsertRead(t *testing.T) {
	tmp := "/tmp/tiedot_col_test"
	os.Remove(tmp)
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
	if doc0 := col.Read(ids[0]); doc0 == nil || strings.Trim(string(doc0), "\000") != string(docs[0]) {
		t.Errorf("Failed to read")
	}
	if doc1 := col.Read(ids[1]); doc1 == nil || strings.Trim(string(doc1), "\000") != string(docs[1]) {
		t.Errorf("Failed to read")
	}
	col.File.Close()
}

func TestInsertUpdateRead(t *testing.T) {
	tmp := "/tmp/tiedot_col_test"
	os.Remove(tmp)
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
	if updated[0], err = col.Update(ids[0], []byte("abcdef")); err != nil || updated[0] != ids[0] {
		t.Errorf("Failed to update: %v", err)
	}
	if updated[1], err = col.Update(ids[1], []byte("longlonglonglonglong")); err != nil || updated[1] == ids[1] {
		t.Errorf("Failed to update: %v", err)
	}
	if doc0 := col.Read(updated[0]); doc0 == nil || strings.Trim(string(doc0), "\000") != "abcdef" {
		t.Errorf("Failed to read")
	}
	if doc1 := col.Read(updated[1]); doc1 == nil || strings.Trim(string(doc1), "\000") != "longlonglonglonglong" {
		t.Errorf("Failed to read")
	}
	col.File.Close()
}

func TestInsertDeleteRead(t *testing.T) {
	tmp := "/tmp/tiedot_col_test"
	os.Remove(tmp)
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
	if doc0 := col.Read(ids[0]); doc0 == nil || strings.Trim(string(doc0), "\000") != string(docs[0]) {
		t.Errorf("Failed to read")
	}
	col.Delete(ids[1])
	if doc1 := col.Read(ids[1]); doc1 != nil {
		t.Errorf("Did not delete")
	}
	if doc2 := col.Read(ids[2]); doc2 == nil || strings.Trim(string(doc2), "\000") != string(docs[2]) {
		t.Errorf("Failed to read")
	}
	col.File.Close()
}

func BenchmarkInsert(b *testing.B) {
	tmp := "/tmp/tiedot_benchmark_insert"
	os.Remove(tmp)
	defer os.Remove(tmp)
	col, err := OpenCol(tmp)
	if err != nil {
		b.Errorf("Failed to open: %v", err)
	}
	load := []byte("abcdefghijklmnopqrstuvwxyz")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err = col.Insert(load); err != nil {
			b.Errorf("Failed to insert: %v", err)
		}
	}
}

func BenchmarkRead(b *testing.B) {
	// Open collection
	tmp := "/tmp/tiedot_benchmark_read"
	os.Remove(tmp)
	defer os.Remove(tmp)
	col, err := OpenCol(tmp)
	if err != nil {
		b.Errorf("Failed to open: %v", err)
	}
	// Insert 1 million documents
	load := []byte("abcdefghijklmnopqrstuvwxyz")
	ids := make([]uint64, COL_BENCH_SIZE)
	for id := range ids {
		if ids[id], err = col.Insert(load); err != nil {
			b.Errorf("Failed to insert: %v", err)
		}
	}
	// Read documents at random ID
	rand.Seed(time.Now().UTC().UnixNano())
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if doc := col.Read(ids[rand.Int63n(COL_BENCH_SIZE)]); doc == nil {
			b.Errorf("Failed to read")
		}
	}
}

func BenchmarkUpdate(b *testing.B) {
	// Open collection
	tmp := "/tmp/tiedot_benchmark_update"
	os.Remove(tmp)
	defer os.Remove(tmp)
	col, err := OpenCol(tmp)
	if err != nil {
		b.Errorf("Failed to open: %v", err)
	}
	// Insert 10 million documents
	load := []byte("abcdefghijklmnopqrstuvwxyz")
	ids := make([]uint64, COL_BENCH_SIZE)
	for id := range ids {
		if ids[id], err = col.Insert(load); err != nil {
			b.Errorf("Failed to insert: %v", err)
		}
	}
	// Update document at random ID
	rand.Seed(time.Now().UTC().UnixNano())
	newDoc := []byte("0123456789")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err = col.Update(ids[rand.Int63n(COL_BENCH_SIZE)], newDoc); err != nil {
			b.Errorf("Failed to update: %v", err)
		}
	}
}

func BenchmarkDelete(b *testing.B) {
	// Open collection
	tmp := "/tmp/tiedot_benchmark_delete"
	os.Remove(tmp)
	defer os.Remove(tmp)
	col, err := OpenCol(tmp)
	if err != nil {
		b.Errorf("Failed to open: %v", err)
	}
	// Insert 1 million documents
	load := []byte("abcdefghijklmnopqrstuvwxyz")
	ids := make([]uint64, COL_BENCH_SIZE)
	for id := range ids {
		if ids[id], err = col.Insert(load); err != nil {
			b.Errorf("Failed to insert: %v", err)
		}
	}
	// Update documents using random ID
	rand.Seed(time.Now().UTC().UnixNano())
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		col.Delete(ids[rand.Int63n(COL_BENCH_SIZE)])
	}
}
