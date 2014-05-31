package data

import (
	"fmt"
	"os"
	"testing"
)

func TestPutGetReopenClear(t *testing.T) {
	tmp := "/tmp/tiedot_test_hash"
	os.Remove(tmp)
	defer os.Remove(tmp)
	ht, err := OpenHashTable(tmp)
	if err != nil {
		t.Fatalf("Failed to open: %v", err)
	}
	// Test initial size information
	if !(ht.numBuckets == INITIAL_BUCKETS && ht.Used == INITIAL_BUCKETS*BUCKET_SIZE && ht.Size == HT_FILE_GROWTH) {
		t.Fatal("Wrong size", ht.numBuckets, INITIAL_BUCKETS, ht.Used, INITIAL_BUCKETS*BUCKET_SIZE, ht.Size, HT_FILE_GROWTH)
	}
	for i := int(0); i < 1024*1024; i++ {
		ht.Put(i, i)
	}
	for i := int(0); i < 1024*1024; i++ {
		vals := ht.Get(i, 0)
		if !(len(vals) == 1 && vals[0] == i) {
			t.Fatalf("Get failed on key %d, got %v", i, vals)
		}
	}
	numBuckets := ht.numBuckets
	// Reopen the hash table and test the features
	if ht.Close(); err != nil {
		panic(err)
	}
	reopened, err := OpenHashTable(tmp)
	if err != nil {
		t.Fatalf("Failed to open: %v", err)
	}
	if reopened.numBuckets != numBuckets {
		t.Fatalf("Wrong.numBuckets")
	}
	if reopened.Used != numBuckets*BUCKET_SIZE {
		t.Fatalf("Wrong UsedSize")
	}
	for i := int(0); i < 1024*1024; i++ {
		vals := reopened.Get(i, 0)
		if !(len(vals) == 1 && vals[0] == i) {
			t.Fatalf("Get failed on key %d, got %v", i, vals)
		}
	}
	// Clear the hash table
	if err = reopened.Clear(); err != nil {
		t.Fatal(err)
	}
	if !(reopened.numBuckets == INITIAL_BUCKETS && reopened.Used == INITIAL_BUCKETS*BUCKET_SIZE) {
		t.Fatal("Did not clear the hash table")
	}
	allKV := make(map[int]int)
	for i := 0; i < 10; i++ {
		keys, vals := reopened.GetPartition(i, 10)
		for i, key := range keys {
			allKV[key] = vals[i]
		}
	}
	if len(allKV) != 0 {
		t.Fatal("Did not clear the hash table")
	}
	if err = reopened.Sync(); err != nil {
		t.Fatal(err)
	}
	if err = reopened.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestPutGet2(t *testing.T) {
	tmp := "/tmp/tiedot_test_hash"
	os.Remove(tmp)
	defer os.Remove(tmp)
	ht, err := OpenHashTable(tmp)
	if err != nil {
		t.Fatalf("Failed to open: %v", err)
		return
	}
	defer ht.Close()
	ht.Put(1, 1)
	ht.Put(1, 2)
	ht.Put(1, 3)
	ht.Put(2, 1)
	ht.Put(2, 2)
	ht.Put(2, 3)
	vals := ht.Get(1, 0)
	if !(len(vals) == 3) {
		t.Fatalf("Get failed, got %v", vals)
	}
	vals = ht.Get(2, 2)
	if !(len(vals) == 2) {
		t.Fatalf("Get failed, got %v", vals)
	}
}

func TestPutRemove(t *testing.T) {
	tmp := "/tmp/tiedot_test_hash"
	os.Remove(tmp)
	defer os.Remove(tmp)
	ht, err := OpenHashTable(tmp)
	if err != nil {
		t.Fatalf("Failed to open: %v", err)
		return
	}
	defer ht.Close()
	ht.Put(1, 1)
	ht.Put(1, 2)
	ht.Put(1, 3)
	ht.Put(2, 1)
	ht.Put(2, 2)
	ht.Put(2, 3)
	ht.Remove(1, 1)
	ht.Remove(2, 2)
	vals := ht.Get(1, 0)
	if !(len(vals) == 2) {
		t.Fatalf("Did not delete, still have %v", vals)
	}
	vals = ht.Get(2, 0)
	if !(len(vals) == 2) {
		t.Fatalf("Did not delete, still have %v", vals)
	}
}

func TestPartitionEntries(t *testing.T) {
	tmp := "/tmp/tiedot_test_hash"
	os.Remove(tmp)
	defer os.Remove(tmp)
	ht, err := OpenHashTable(tmp)
	if err != nil {
		t.Fatalf("Failed to open: %v", err)
		return
	}
	number := 100000
	for i := 0; i < number; i++ {
		ht.Put(i, i*2)
	}
	allKV := make(map[int]int)
	for i := 0; i < 100; i++ {
		start, end := GetPartitionRange(i, 100)
		keys, vals := ht.GetPartition(i, 100)
		fmt.Println("Between ", start, end, " there are ", len(keys))
		for i, key := range keys {
			allKV[key] = vals[i]
		}
	}
	// Verify read back
	if len(allKV) != number {
		t.Fatal("Not enough entries, only got ", len(allKV))
	}
	for i := 0; i < number; i++ {
		if allKV[i] != i*2 {
			t.Fatal("Wrong readback", i, allKV[i])
		}
	}
}
