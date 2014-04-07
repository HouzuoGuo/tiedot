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
		t.Fatal("Wrong size")
	}
	fmt.Println("Please be patient, this may take a minute.")
	for i := int(0); i < 1024*1024*4; i++ {
		ht.Put(i, i)
	}
	for i := int(0); i < 1024*1024*4; i++ {
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
	for i := int(0); i < 1024*1024*4; i++ {
		vals := reopened.Get(i, 0)
		if !(len(vals) == 1 && vals[0] == i) {
			t.Fatalf("Get failed on key %d, got %v", i, vals)
		}
	}
	// Clear the hash table
	reopened.Clear()
	if !(reopened.numBuckets == INITIAL_BUCKETS && reopened.Used == INITIAL_BUCKETS*BUCKET_SIZE) {
		t.Fatal("Did not clear the hash table")
	}
	keys, vals := reopened.GetAll(0)
	if len(keys) != 0 || len(vals) != 0 {
		t.Fatal("Did not clear the hash table")
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

func TestGetAll(t *testing.T) {
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
	ht.Put(2, 3)
	ht.Put(2, 3)
	ht.Put(2, 3)
	ht.Put(2, 3)
	ht.Put(2, 3)
	ht.Put(2, 3)
	ht.Put(2, 3)
	ht.Put(2, 3)
	ht.Put(2, 3)
	ht.Put(2, 3)
	keys, vals := ht.GetAll(0)
	if !(len(keys) == 16 && len(vals) == 16) {
		t.Fatalf("Did not get everything, got only %v, %v", keys, vals)
	}
	keys, vals = ht.GetAll(3)
	if !(len(keys) == 3 && len(vals) == 3) {
		t.Fatalf("Did not get three values, got %v, %v", keys, vals)
	}
}
