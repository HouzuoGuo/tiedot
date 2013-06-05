package file

import (
	"os"
	"testing"
	"fmt"
)

func TestPutGet(t *testing.T) {
	tmp := "/tmp/tiedot_hash_test"
	os.Remove(tmp)
	defer os.Remove(tmp)
	ht, err := OpenHash(tmp, 3, 3)
	if err != nil {
		t.Errorf("Failed to open: %v", err)
	}
	for i := uint64(0); i < 20; i++ {
		ht.Put(i, i)
	}
	fmt.Println("Put completed")
	for i := uint64(0); i < 20; i++ {
		keys, vals := ht.Get(i, 1, func(a, b uint64) bool {
			return true
		})
		if !(cap(keys) == 1 && keys[0] == i && cap(vals) == 1 && vals[0] == i) {
			t.Errorf("Get failed on key %d", i)
		}
	}
	ht.File.Close()
}

func TestPutGet2(t *testing.T) {
}

func TestPutRemove(t *testing.T) {
}

func TestGetAll(t *testing.T) {
}
