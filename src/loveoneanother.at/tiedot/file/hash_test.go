package file

import (
	"math/rand"
	"os"
	"testing"
	"time"
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
	ht.Remove(1, 1, func(a, b uint64) bool {
		return true
	})
	ht.Remove(2, 2, func(a, b uint64) bool {
		return b >= 2
	})
	keys, vals := ht.Get(1, 0, func(a, b uint64) bool {
		return true
	})
	if !(len(keys) == 2 && len(vals) == 2) {
		t.Fatalf("Did not delete, still have %v, %v", keys, vals)
	}
	keys, vals = ht.Get(2, 0, func(a, b uint64) bool {
		return true
	})
	if !(len(keys) == 1 && len(vals) == 1) {
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

func BenchmarkPut(b *testing.B) {
	tmp := "/tmp/tiedot_hash_benchmark"
	os.Remove(tmp)
	defer os.Remove(tmp)
	ht, err := OpenHash(tmp, 14, 100)
	if err != nil {
		b.Fatalf("Failed to open: %v", err)
		return
	}
	defer ht.File.Close()
	rand.Seed(time.Now().UTC().UnixNano())
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ht.Put(uint64(rand.Int63n(HT_BENCH_SIZE)), uint64(rand.Int63n(HT_BENCH_SIZE)))
	}
}

func BenchmarkGet(b *testing.B) {
	tmp := "/tmp/tiedot_hash_benchmark"
	os.Remove(tmp)
	defer os.Remove(tmp)
	ht, err := OpenHash(tmp, 14, 100)
	if err != nil {
		b.Fatalf("Failed to open: %v", err)
		return
	}
	defer ht.File.Close()
	rand.Seed(time.Now().UTC().UnixNano())
	for i := 0; i < HT_BENCH_SIZE; i++ {
		ht.Put(uint64(rand.Int63n(HT_BENCH_SIZE)), uint64(rand.Int63n(HT_BENCH_SIZE)))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ht.Get(uint64(rand.Int63n(HT_BENCH_SIZE)), 1, func(a, b uint64) bool {
			return true
		})
	}
}

func BenchmarkRemove(b *testing.B) {
	tmp := "/tmp/tiedot_hash_benchmark"
	os.Remove(tmp)
	defer os.Remove(tmp)
	ht, err := OpenHash(tmp, 14, 100)
	if err != nil {
		b.Fatalf("Failed to open: %v", err)
		return
	}
	defer ht.File.Close()
	rand.Seed(time.Now().UTC().UnixNano())
	for i := 0; i < HT_BENCH_SIZE; i++ {
		ht.Put(uint64(rand.Int63n(HT_BENCH_SIZE)), uint64(rand.Int63n(HT_BENCH_SIZE)))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ht.Remove(uint64(rand.Int63n(HT_BENCH_SIZE)), 1, func(a, b uint64) bool {
			return true
		})
	}
}

func BenchmarkGetAll(b *testing.B) {
	tmp := "/tmp/tiedot_hash_benchmark"
	os.Remove(tmp)
	defer os.Remove(tmp)
	ht, err := OpenHash(tmp, 12, 400)
	if err != nil {
		b.Fatalf("Failed to open: %v", err)
		return
	}
	defer ht.File.Close()
	rand.Seed(time.Now().UTC().UnixNano())
	for i := 0; i < HT_BENCH_SIZE; i++ {
		ht.Put(uint64(rand.Int63n(HT_BENCH_SIZE)), uint64(rand.Int63n(HT_BENCH_SIZE)))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ht.GetAll(0)
	}
}
