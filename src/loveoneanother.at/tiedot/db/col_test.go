package db

import (
	"encoding/json"
	"math/rand"
	"os"
	"testing"
	"time"
)

const (
	COL_BENCH_SIZE = 1000000 // Number of documents made available for collection benchmark
)

func TestInsertRead(t *testing.T) {
	tmp := "/tmp/tiedot_col_test"
	os.RemoveAll(tmp)
	defer os.RemoveAll(tmp)
	col, err := OpenCol(tmp)
	if err != nil {
		t.Errorf("Failed to open: %v", err)
	}
	docs := []string{`{"a": 1}`, `{"b": 2}`}
	var jsonDoc [2]interface{}
	json.Unmarshal([]byte(docs[0]), &jsonDoc[0])
	json.Unmarshal([]byte(docs[1]), &jsonDoc[1])

	ids := [2]uint64{}
	if ids[0], err = col.Insert(jsonDoc[0]); err != nil {
		t.Errorf("Failed to insert: %v", err)
	}
	if ids[1], err = col.Insert(jsonDoc[1]); err != nil {
		t.Errorf("Failed to insert: %v", err)
	}
	if col.Read(ids[0]).(map[string]interface{})[string('a')].(float64) != 1.0 {
		t.Errorf("Failed to read back document, got %v", col.Read(ids[0]))
	}
	if col.Read(ids[1]).(map[string]interface{})[string('b')].(float64) != 2.0 {
		t.Errorf("Failed to read back document, got %v", col.Read(ids[1]))
	}
	keys, vals := col.IdIndex.GetAll()
	if !(keys[0] == ids[0] && ids[0] == vals[0] && keys[1] == ids[1] && ids[1] == vals[1] && len(keys) == 2 && len(vals) == 2) {
		t.Errorf("ID Index was not set correctly")
	}
}

func TestInsertUpdateRead(t *testing.T) {
	tmp := "/tmp/tiedot_col_test"
	os.RemoveAll(tmp)
	defer os.RemoveAll(tmp)
	col, err := OpenCol(tmp)
	if err != nil {
		t.Errorf("Failed to open: %v", err)
	}

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
		t.Errorf("Failed to insert: %v", err)
	}
	if ids[1], err = col.Insert(jsonDoc[1]); err != nil {
		t.Errorf("Failed to insert: %v", err)
	}

	if ids[0], err = col.Update(ids[0], updatedJsonDoc[0]); err != nil {
		t.Errorf("Failed to update: %v", err)
	}
	if ids[1], err = col.Update(ids[1], updatedJsonDoc[1]); err != nil {
		t.Errorf("Failed to update: %v", err)
	}

	if col.Read(ids[0]).(map[string]interface{})[string('a')].(float64) != 2.0 {
		t.Errorf("Failed to read back document, got %v", col.Read(ids[0]))
	}
	if col.Read(ids[1]).(map[string]interface{})[string('b')].(string) != string("abcdefghijklmnopqrstuvwxyz") {
		t.Errorf("Failed to read back document, got %v", col.Read(ids[1]))
	}
	keys, vals := col.IdIndex.GetAll()
	if !(keys[0] == ids[0] && ids[0] == vals[0] && keys[1] == ids[1] && ids[1] == vals[1] && len(keys) == 2 && len(vals) == 2) {
		t.Errorf("ID Index was not set correctly")
	}
}

func TestInsertDeleteRead(t *testing.T) {
	tmp := "/tmp/tiedot_col_test"
	os.RemoveAll(tmp)
	defer os.RemoveAll(tmp)
	col, err := OpenCol(tmp)
	if err != nil {
		t.Errorf("Failed to open: %v", err)
	}
	docs := []string{`{"a": 1}`, `{"b": 2}`}
	var jsonDoc [2]interface{}
	json.Unmarshal([]byte(docs[0]), &jsonDoc[0])
	json.Unmarshal([]byte(docs[1]), &jsonDoc[1])
	ids := [2]uint64{}
	if ids[0], err = col.Insert(jsonDoc[0]); err != nil {
		t.Errorf("Failed to insert: %v", err)
	}
	if ids[1], err = col.Insert(jsonDoc[1]); err != nil {
		t.Errorf("Failed to insert: %v", err)
	}
	col.Delete(ids[0])
	if col.Read(ids[0]) != nil {
		t.Errorf("Did not delete document, still read %v", col.Read(ids[0]))
	}
	if col.Read(ids[1]).(map[string]interface{})[string('b')].(float64) != 2 {
		t.Errorf("Failed to read back document, got %v", col.Read(ids[1]))
	}
	keys, vals := col.IdIndex.GetAll()
	if !(keys[0] == ids[1] && ids[1] == vals[0] && len(keys) == 1 && len(vals) == 1) {
		t.Errorf("ID Index was not set correctly")
	}
}

func TestIndex(t *testing.T) {
	tmp := "/tmp/tiedot_col_test"
	os.RemoveAll(tmp)
	defer os.RemoveAll(tmp)
	col, err := OpenCol(tmp)
	if err != nil {
		t.Errorf("Failed to open: %v", err)
	}
	docs := []string{`{"a": {"b": {"c": 1}, "d": 0}}`, `{"a": {"b": {"c": 2}, "d": 0}}`, `{"a": {"b": {"c": 3}, "d": 0}}`, `{"a": {"b": {"c": 4}, "d": 0}}`}
	var jsonDoc [4]interface{}
	json.Unmarshal([]byte(docs[0]), &jsonDoc[0])
	json.Unmarshal([]byte(docs[1]), &jsonDoc[1])
	json.Unmarshal([]byte(docs[2]), &jsonDoc[2])
	json.Unmarshal([]byte(docs[3]), &jsonDoc[3])
	ids := [3]uint64{}
	ids[0], _ = col.Insert(jsonDoc[0])
	if err = col.Index([]string{"a", "b", "c"}); err != nil {
		t.Error(err)
	}
	if err = col.Index([]string{"d"}); err != nil {
		t.Error(err)
	}
	for _, first := range col.StrHT {
		keys, vals := first.GetAll()
		if !(len(keys) == 1 && len(vals) == 1 && keys[0] == StrHash(1) && vals[0] == ids[0]) {
			t.Errorf("Did not index existing document")
		}
		break
	}
	ids[1], _ = col.Insert(jsonDoc[1])
	ids[2], _ = col.Insert(jsonDoc[2])
	//	ids[2], _ = col.Update(ids[2], jsonDoc[3])
	//	col.Delete(ids[1])
	index1 := col.StrHT["a,b,c"]
	index2 := col.StrHT["d"]
	t.Error(index1.GetAll())
	t.Error(index2.GetAll())
}

func BenchmarkInsert(b *testing.B) {
	tmp := "/tmp/tiedot_col_bench"
	os.RemoveAll(tmp)
	defer os.RemoveAll(tmp)
	col, err := OpenCol(tmp)
	if err != nil {
		b.Errorf("Failed to open: %v", err)
	}
	var jsonDoc interface{}
	json.Unmarshal([]byte(`{"a": 1}`), &jsonDoc)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		col.Insert(jsonDoc)
	}
}

func BenchmarkRead(b *testing.B) {
	tmp := "/tmp/tiedot_col_bench"
	os.RemoveAll(tmp)
	defer os.RemoveAll(tmp)
	col, err := OpenCol(tmp)
	if err != nil {
		b.Errorf("Failed to open: %v", err)
	}
	var jsonDoc interface{}
	json.Unmarshal([]byte(`{"a": 1}`), &jsonDoc)
	var ids [COL_BENCH_SIZE]uint64
	for i := 0; i < COL_BENCH_SIZE; i++ {
		ids[i], _ = col.Insert(jsonDoc)
	}
	rand.Seed(time.Now().UTC().UnixNano())
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		col.Read(ids[rand.Int63n(COL_BENCH_SIZE)])
	}
}

func BenchmarkUpdate(b *testing.B) {
	tmp := "/tmp/tiedot_col_bench"
	os.RemoveAll(tmp)
	defer os.RemoveAll(tmp)
	col, err := OpenCol(tmp)
	if err != nil {
		b.Errorf("Failed to open: %v", err)
	}
	var jsonDoc interface{}
	json.Unmarshal([]byte(`{"a": 1}`), &jsonDoc)
	var ids [COL_BENCH_SIZE]uint64
	for i := 0; i < COL_BENCH_SIZE; i++ {
		ids[i], _ = col.Insert(jsonDoc)
	}
	rand.Seed(time.Now().UTC().UnixNano())
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		col.Update(ids[rand.Int63n(COL_BENCH_SIZE)], jsonDoc)
	}
}

func BenchmarkDelete(b *testing.B) {
	tmp := "/tmp/tiedot_col_bench"
	os.RemoveAll(tmp)
	defer os.RemoveAll(tmp)
	col, err := OpenCol(tmp)
	if err != nil {
		b.Errorf("Failed to open: %v", err)
	}
	var jsonDoc interface{}
	json.Unmarshal([]byte(`{"a": 1}`), &jsonDoc)
	var ids [COL_BENCH_SIZE]uint64
	for i := 0; i < COL_BENCH_SIZE; i++ {
		ids[i], _ = col.Insert(jsonDoc)
	}
	rand.Seed(time.Now().UTC().UnixNano())
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		col.Delete(ids[rand.Int63n(COL_BENCH_SIZE)])
	}
}
