package file

import (
	"fmt"
	"math/rand"
	"os"
	"strings"
	"testing"
	"time"
)

const COL_BENCH_SIZE = 200000 // Number of documents made available for collection benchmark

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
	if updated[1], err = col.Update(ids[1], []byte("longlonglonglonglong")); err != nil || updated[1] == ids[1] {
		t.Fatalf("Failed to update: %v", err)
	}
	if doc0 := col.Read(updated[0]); doc0 == nil || strings.TrimSpace(string(doc0)) != "abcdef" {
		t.Fatalf("Failed to read")
	}
	if doc1 := col.Read(updated[1]); doc1 == nil || strings.TrimSpace(string(doc1)) != "longlonglonglonglong" {
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

func BenchmarkInsert(b *testing.B) {
	tmp := "/tmp/tiedot_benchmark_insert"
	os.Remove(tmp)
	defer os.Remove(tmp)
	col, err := OpenCol(tmp)
	if err != nil {
		b.Fatalf("Failed to open: %v", err)
		return
	}
	defer col.File.Close()
	load := []byte("abcdefghijklmnopqrstuvwxyz")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		col.Insert(load)
	}
}

func BenchmarkRead(b *testing.B) {
	// Open collection
	tmp := "/tmp/tiedot_benchmark_read"
	os.Remove(tmp)
	defer os.Remove(tmp)
	col, err := OpenCol(tmp)
	if err != nil {
		b.Fatalf("Failed to open: %v", err)
		return
	}
	defer col.File.Close()
	// Insert 1 million documents
	load := []byte("abcdefghijklmnopqrstuvwxyz")
	ids := make([]uint64, COL_BENCH_SIZE)
	for id := range ids {
		if ids[id], err = col.Insert(load); err != nil {
			b.Fatalf("Failed to insert: %v", err)
		}
	}
	// Read documents at random ID
	rand.Seed(time.Now().UTC().UnixNano())
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		col.Read(ids[rand.Int63n(COL_BENCH_SIZE)])
	}
}

func BenchmarkUpdate(b *testing.B) {
	// Open collection
	tmp := "/tmp/tiedot_benchmark_update"
	os.Remove(tmp)
	defer os.Remove(tmp)
	col, err := OpenCol(tmp)
	if err != nil {
		b.Fatalf("Failed to open: %v", err)
		return
	}
	defer col.File.Close()
	// Insert 10 million documents
	load := []byte("abcdefghijklmnopqrstuvwxyz")
	ids := make([]uint64, COL_BENCH_SIZE)
	for id := range ids {
		if ids[id], err = col.Insert(load); err != nil {
			b.Fatalf("Failed to insert: %v", err)
		}
	}
	// Update document at random ID
	rand.Seed(time.Now().UTC().UnixNano())
	newDoc := []byte("0123456789")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		col.Update(ids[rand.Int63n(COL_BENCH_SIZE)], newDoc)
	}
}

func BenchmarkDelete(b *testing.B) {
	// Open collection
	tmp := "/tmp/tiedot_benchmark_delete"
	os.Remove(tmp)
	defer os.Remove(tmp)
	col, err := OpenCol(tmp)
	if err != nil {
		b.Fatalf("Failed to open: %v", err)
		return
	}
	defer col.File.Close()
	// Insert 1 million documents
	load := []byte("abcdefghijklmnopqrstuvwxyz")
	ids := make([]uint64, COL_BENCH_SIZE)
	for _ = range ids {
		col.Insert(load)
	}
	// Update documents using random ID
	rand.Seed(time.Now().UTC().UnixNano())
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		col.Delete(ids[rand.Int63n(COL_BENCH_SIZE)])
	}
}

func BenchmarkColGetAll(b *testing.B) {
	tmp := "/tmp/tiedot_benchmark_getall"
	os.Remove(tmp)
	defer os.Remove(tmp)
	col, err := OpenCol(tmp)
	if err != nil {
		b.Fatalf("Failed to open: %v", err)
		return
	}
	defer col.File.Close()
	load := []byte("abcdefghijklmnopqrstuvwxyz")
	ids := make([]uint64, COL_BENCH_SIZE)
	for _ = range ids {
		if _, err = col.Insert(load); err != nil {
			b.Fatal(err)
		}
	}
	rand.Seed(time.Now().UTC().UnixNano())
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		col.ForAll(func(id uint64, doc []byte) bool {
			return true
		})
	}
}
