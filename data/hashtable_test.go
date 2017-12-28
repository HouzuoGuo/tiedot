package data

import (
	"encoding/binary"
	"math"
	"os"
	"reflect"
	"testing"

	"github.com/bouk/monkey"
	"github.com/pkg/errors"
)

func TestPutGetReopenClear(t *testing.T) {
	tmp := "/tmp/tiedot_test_hash"
	os.Remove(tmp)
	defer os.Remove(tmp)
	d := defaultConfig()
	ht, err := d.OpenHashTable(tmp)
	if err != nil {
		t.Fatalf("Failed to open: %v", err)
	}
	// Test initial size information
	if !(ht.numBuckets == d.InitialBuckets && ht.Used == d.InitialBuckets*d.BucketSize && ht.Size == d.HTFileGrowth) {
		t.Fatal("Wrong size", ht.numBuckets, d.InitialBuckets, ht.Used, d.InitialBuckets*d.BucketSize, ht.Size, d.HTFileGrowth)
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
	reopened, err := d.OpenHashTable(tmp)
	if err != nil {
		t.Fatalf("Failed to open: %v", err)
	}
	if reopened.numBuckets != numBuckets {
		t.Fatalf("Wrong numBuckets %d, expected %d", reopened.numBuckets, numBuckets)
	}
	if reopened.Used != numBuckets*d.BucketSize {
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
	if !(reopened.numBuckets == d.InitialBuckets && reopened.Used == d.InitialBuckets*d.BucketSize) {
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
	if err = reopened.Close(); err != nil {
		t.Fatal(err)
	}
}
func TestPutGet2(t *testing.T) {
	tmp := "/tmp/tiedot_test_hash"
	os.Remove(tmp)
	defer os.Remove(tmp)
	d := defaultConfig()
	ht, err := d.OpenHashTable(tmp)
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
	d := defaultConfig()
	ht, err := d.OpenHashTable(tmp)
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
	d := defaultConfig()
	ht, err := d.OpenHashTable(tmp)
	if err != nil {
		t.Fatalf("Failed to open: %v", err)
		return
	}
	defer ht.Close()
	number := 2000000
	for i := 1; i <= number; i++ {
		ht.Put(i, i*2)
		if gotBack := ht.Get(i, 0); len(gotBack) != 1 || gotBack[0] != i*2 {
			t.Fatal("Written ", i, i*2, "got back", gotBack)
		}
	}
	for parts := 2; parts < 19; parts++ {
		t.Log("parts is", parts)
		allKV := make(map[int]int)
		counter := 0
		for i := 0; i < parts; i++ {
			start, end := d.GetPartitionRange(i, parts)
			keys, vals := ht.GetPartition(i, parts)
			t.Log("Between ", start, end, " there are ", len(keys))
			sizeDev := math.Abs(float64(len(keys)-number/parts)) / float64(number/parts)
			t.Log("sizeDev", sizeDev)
			if sizeDev > 0.1 {
				t.Fatal("imbalanced keys")
			}
			for i, key := range keys {
				allKV[key] = vals[i]
			}
			counter += len(keys)
		}
		// Verify read back
		if counter != number {
			t.Fatal("Number of entries does not match, got ", counter)
		}
		for i := 0; i < number; i++ {
			if allKV[i] != i*2 {
				t.Fatal("Wrong readback", i, allKV[i])
			}
		}
	}
}
func TestOpenHashTableErr(t *testing.T) {
	errMessage := "Error open data file"
	patch := monkey.Patch(OpenDataFile, func(path string, growth int) (file *DataFile, err error) {
		return nil, errors.New(errMessage)
	})
	defer patch.Unpatch()

	d := defaultConfig()
	if _, err := d.OpenHashTable(""); err.Error() != errMessage {
		t.Error("Expected error open data file")
	}
}
func TestRemoveEntryKeyZero(t *testing.T) {
	os.Remove(tmp)
	defer os.Remove(tmp)
	var d *Config

	patch := monkey.PatchInstanceMethod(reflect.TypeOf(d), "HashKey", func(_ *Config, key int) int {
		return 0
	})
	defer patch.Unpatch()

	d = defaultConfig()
	hash, _ := d.OpenHashTable(tmp)
	hash.HashKey(1)

	hash.Put(1, 1)
	hash.Remove(2, 1)
}
func TestRemoveEqualZeroPerBucket(t *testing.T) {
	os.Remove(tmp)
	defer os.Remove(tmp)
	var d *Config

	patch := monkey.PatchInstanceMethod(reflect.TypeOf(d), "HashKey", func(_ *Config, key int) int {
		return 0
	})
	defer patch.Unpatch()

	d = defaultConfig()
	hash, _ := d.OpenHashTable(tmp)
	hash.HashKey(1)
	hash.Put(1, 1)

	patchVarint := monkey.Patch(binary.Varint, func(buf []byte) (int64, int) {
		return 1, 0
	})
	defer patchVarint.Unpatch()

	hash.Remove(1, 0)
}
func TestCalculateNumBucketsSizeOver(t *testing.T) {
	os.Remove(tmp)
	defer os.Remove(tmp)
	patch := monkey.Patch(OpenDataFile, func(path string, growth int) (file *DataFile, err error) {
		return &DataFile{
			Path:   path,
			Growth: growth,
			Size:   0,
		}, nil
	})
	defer patch.Unpatch()
	d := defaultConfig()
	d.OpenHashTable(tmp)
}
func TestNextBucketZero(t *testing.T) {
	os.Remove(tmp)
	defer os.Remove(tmp)
	d := defaultConfig()
	hash, _ := d.OpenHashTable(tmp)
	if hash.nextBucket(hash.numBuckets+1) != 0 {
		t.Error("Expected zero if bucket argument more hash numBuckets")
	}

}
