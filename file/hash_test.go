package file

import (
	"os"
	"testing"
)

const (
	HT_BENCH_SIZE = 1000000 // Number of entries made available for hash table benchmark
)

func TestPutGet(t *testing.T) {
	tmp := "/tmp/tiedot_hash_test"
	os.Remove(tmp)
	defer os.Remove(tmp)
	ht, err := OpenHash(tmp, 2, 2)
	if err != nil {
		t.Fatalf("Failed to open: %v", err)
		return
	}
	defer ht.File.Close()
	for i := uint64(0); i < 30; i++ {
		ht.Put(i, i)
	}
	for i := uint64(0); i < 30; i++ {
		keys, vals := ht.Get(i, 0, func(a, b uint64) bool {
			return true
		})
		if !(len(keys) == 1 && keys[0] == i && len(vals) == 1 && vals[0] == i) {
			t.Fatalf("Get failed on key %d, got %v and %v", i, keys, vals)
		}
	}
}

func TestPutGet2(t *testing.T) {
	tmp := "/tmp/tiedot_hash_test"
	os.Remove(tmp)
	defer os.Remove(tmp)
	ht, err := OpenHash(tmp, 2, 2)
	if err != nil {
		t.Fatalf("Failed to open: %v", err)
		return
	}
	defer ht.File.Close()
	ht.Put(1, 1)
	ht.Put(1, 2)
	ht.Put(1, 3)
	ht.Put(2, 1)
	ht.Put(2, 2)
	ht.Put(2, 3)
	keys, vals := ht.Get(1, 0, func(a, b uint64) bool {
		return true
	})
	if !(len(keys) == 3 && len(vals) == 3) {
		t.Fatalf("Get failed, got %v, %v", keys, vals)
	}
	keys, vals = ht.Get(2, 2, func(a, b uint64) bool {
		return true
	})
	if !(len(keys) == 2 && len(vals) == 2) {
		t.Fatalf("Get failed, got %v, %v", keys, vals)
	}
	keys, vals = ht.Get(1, 0, func(a, b uint64) bool {
		return b >= 2
	})
	if !(len(keys) == 2 && len(vals) == 2) {
		t.Fatalf("Get failed, got %v, %v", keys, vals)
	}
}

func TestPutRemove(t *testing.T) {
	tmp := "/tmp/tiedot_hash_test"
	os.Remove(tmp)
	defer os.Remove(tmp)
	ht, err := OpenHash(tmp, 2, 2)
	if err != nil {
		t.Fatalf("Failed to open: %v", err)
		return
	}
	defer ht.File.Close()
	ht.Put(1, 1)
	ht.Put(1, 2)
	ht.Put(1, 3)
	ht.Put(2, 1)
	ht.Put(2, 2)
	ht.Put(2, 3)
	ht.Remove(1, 1)
	ht.Remove(2, 2)
	keys, vals := ht.Get(1, 0, func(a, b uint64) bool {
		return true
	})
	if !(len(keys) == 2 && len(vals) == 2) {
		t.Fatalf("Did not delete, still have %v, %v", keys, vals)
	}
	keys, vals = ht.Get(2, 0, func(a, b uint64) bool {
		return true
	})
	if !(len(keys) == 2 && len(vals) == 2) {
		t.Fatalf("Did not delete, still have %v, %v", keys, vals)
	}
}

func TestGetAll(t *testing.T) {
	tmp := "/tmp/tiedot_hash_test"
	os.Remove(tmp)
	defer os.Remove(tmp)
	ht, err := OpenHash(tmp, 2, 2)
	if err != nil {
		t.Fatalf("Failed to open: %v", err)
		return
	}
	defer ht.File.Close()
	ht.Put(1, 1)
	ht.Put(1, 2)
	ht.Put(1, 3)
	ht.Put(2, 1)
	ht.Put(2, 2)
	ht.Put(2, 3)
	keys, vals := ht.GetAll(0)
	if !(len(keys) == 6 && len(vals) == 6) {
		t.Fatalf("Did not have all, got only %v, %v", keys, vals)
	}
}
