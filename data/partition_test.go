package data

import (
	"errors"
	"fmt"
	"math/rand"
	"os"
	"reflect"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/HouzuoGuo/tiedot/dberr"
	"github.com/bouk/monkey"
)

func TestPartitionDocCRUD(t *testing.T) {
	colPath := "/tmp/tiedot_test_col"
	htPath := "/tmp/tiedot_test_ht"
	os.Remove(colPath)
	os.Remove(htPath)
	defer os.Remove(colPath)
	defer os.Remove(htPath)
	d := defaultConfig()
	part, err := d.OpenPartition(colPath, htPath)
	if err != nil {
		t.Fatal(err)
	}
	// Insert & read
	if _, err = part.Insert(1, []byte("1")); err != nil {
		t.Fatal(err)
	}
	if _, err = part.Insert(2, []byte("2")); err != nil {
		t.Fatal(err)
	}
	if readback, err := part.Read(1); err != nil || string(readback) != "1 " {
		t.Fatal(err, readback)
	}
	if readback, err := part.Read(2); err != nil || string(readback) != "2 " {
		t.Fatal(err, readback)
	}
	// Update & read
	if err = part.Update(1, []byte("abcdef")); err != nil {
		t.Fatal(err)
	}
	if err := part.Update(1234, []byte("abcdef")); dberr.Type(err) != dberr.ErrorNoDoc {
		t.Fatal("Did not error")
	}
	if readback, err := part.Read(1); err != nil || string(readback) != "abcdef      " {
		t.Fatal(err, readback)
	}
	// Delete & read
	if err = part.Delete(1); err != nil {
		t.Fatal(err)
	}
	if _, err = part.Read(1); dberr.Type(err) != dberr.ErrorNoDoc {
		t.Fatal("Did not error")
	}
	if err = part.Delete(123); dberr.Type(err) != dberr.ErrorNoDoc {
		t.Fatal("Did not error")
	}
	// Foreach
	part.ForEachDoc(0, 1, func(id int, doc []byte) bool {
		if id != 2 || string(doc) != "2 " {
			t.Fatal("ID 2 should be the only remaining document")
		}
		return true
	})
	// Finish up
	if err = part.Clear(); err != nil {
		t.Fatal(err)
	}
	if err = part.Close(); err != nil {
		t.Fatal(err)
	}
}

// Lock & unlock
func TestLock(t *testing.T) {
	d := defaultConfig()
	part := d.newPartition()
	n := 400
	m := map[int]int{}
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		i := i
		go func() {
			part.LockUpdate(123)
			m[i] = i
			part.UnlockUpdate(123)
			wg.Done()
		}()
	}
	wg.Wait()
	if len(m) != n {
		t.Fatal("unexpected map content")
	}
}

func TestApproxDocCount(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	colPath := "/tmp/tiedot_test_col"
	htPath := "/tmp/tiedot_test_ht"
	os.Remove(colPath)
	os.Remove(htPath)
	defer os.Remove(colPath)
	defer os.Remove(htPath)
	d := defaultConfig()
	part, err := d.OpenPartition(colPath, htPath)
	if err != nil {
		t.Fatal(err)
	}
	defer part.Close()
	// Insert 100 documents
	for i := 0; i < 100; i++ {
		if _, err = part.Insert(rand.Int(), []byte(strconv.Itoa(i))); err != nil {
			t.Fatal(err)
		}
	}
	t.Log("ApproxDocCount", part.ApproxDocCount())
	if part.ApproxDocCount() < 10 || part.ApproxDocCount() > 300 {
		t.Fatal("Approximate is way off", part.ApproxDocCount())
	}
	// Insert 900 documents
	for i := 0; i < 900; i++ {
		if _, err = part.Insert(rand.Int(), []byte(strconv.Itoa(i))); err != nil {
			t.Fatal(err)
		}
	}
	t.Log("ApproxDocCount", part.ApproxDocCount())
	if part.ApproxDocCount() < 500 || part.ApproxDocCount() > 1500 {
		t.Fatal("Approximate is way off", part.ApproxDocCount())
	}
	// Insert another 2000 documents
	for i := 0; i < 2000; i++ {
		if _, err = part.Insert(rand.Int(), []byte(strconv.Itoa(i))); err != nil {
			t.Fatal(err)
		}
	}
	t.Log("ApproxDocCount", part.ApproxDocCount())
	if part.ApproxDocCount() < 2000 || part.ApproxDocCount() > 4000 {
		t.Fatal("Approximate is way off", part.ApproxDocCount())
	}
	// See how fast doc count is
	start := time.Now().UnixNano()
	for i := 0; i < 1000; i++ {
		part.ApproxDocCount()
	}
	timediff := time.Now().UnixNano() - start
	t.Log("It took", timediff/1000000, "milliseconds")
	if timediff/1000000 > 10000 {
		t.Fatal("Algorithm is way too slow")
	}
}
func TestOpenPartitionErrOpenCol(t *testing.T) {
	var d *Config
	errMessage := "error open collection"
	patch := monkey.PatchInstanceMethod(reflect.TypeOf(d), "OpenCollection", func(_ *Config, path string) (col *Collection, err error) {
		return nil, errors.New(errMessage)
	})
	defer patch.Unpatch()

	d = defaultConfig()
	if _, err := d.OpenPartition("", ""); errMessage != err.Error() {
		t.Error("Expected error after call `OpenCollection`")
	}
}
func TestOpenPartitionOpenHashTable(t *testing.T) {
	var d *Config
	errMessage := "error open hash table"

	patchCol := monkey.PatchInstanceMethod(reflect.TypeOf(d), "OpenCollection", func(_ *Config, path string) (col *Collection, err error) {
		return &Collection{}, nil
	})
	defer patchCol.Unpatch()

	patch := monkey.PatchInstanceMethod(reflect.TypeOf(d), "OpenHashTable", func(_ *Config, path string) (col *HashTable, err error) {
		return nil, errors.New(errMessage)
	})
	defer patch.Unpatch()

	d = defaultConfig()
	if _, err := d.OpenPartition("", ""); errMessage != err.Error() {
		t.Error("Expected error after call `OpenCollection`")
	}
}
func TestInsertErr(t *testing.T) {
	errMessage := "error insert in collection"
	d := defaultConfig()
	part := d.newPartition()
	var col *Collection
	patch := monkey.PatchInstanceMethod(reflect.TypeOf(col), "Insert", func(_ *Collection, data []byte) (id int, err error) {
		return 0, errors.New(errMessage)
	})
	defer patch.Unpatch()

	if _, err := part.Insert(1, []byte("")); errMessage != err.Error() {
		t.Error("Expected error after call insert data in collection")
	}

}
func TestReadErr(t *testing.T) {
	d := defaultConfig()
	var hash *HashTable
	var col *Collection
	patchHash := monkey.PatchInstanceMethod(reflect.TypeOf(hash), "Get", func(_ *HashTable, key, limit int) (vals []int) {
		return []int{1, 2, 3}
	})
	defer patchHash.Unpatch()

	patchCol := monkey.PatchInstanceMethod(reflect.TypeOf(col), "Read", func(_ *Collection, id int) []byte {
		return nil
	})
	defer patchCol.Unpatch()
	part := d.newPartition()
	if _, err := part.Read(1); err.Error() != fmt.Sprintf(string(dberr.ErrorNoDoc), 1) {
		t.Error("Expected error document does not exist")
	}
}
func TestUpdateErr(t *testing.T) {
	errMessage := "Error update collection"
	var hash *HashTable
	var col *Collection
	patchHash := monkey.PatchInstanceMethod(reflect.TypeOf(hash), "Get", func(_ *HashTable, key, limit int) (vals []int) {
		return []int{1, 2, 3}
	})
	defer patchHash.Unpatch()

	patchCol := monkey.PatchInstanceMethod(reflect.TypeOf(col), "Update", func(_ *Collection, id int, data []byte) (newID int, err error) {
		return 0, errors.New(errMessage)
	})
	defer patchCol.Unpatch()
	d := defaultConfig()
	part := d.newPartition()
	if part.Update(1, []byte("")).Error() != errMessage {
		t.Error("Expected error when call update collection")
	}
}
func TestForEachDocIfCallbackTrue(t *testing.T) {
	var hash *HashTable
	var col *Collection
	patchHash := monkey.PatchInstanceMethod(reflect.TypeOf(hash), "GetPartition", func(_ *HashTable, partNum, partSize int) (keys, vals []int) {
		return []int{1, 2, 3}, []int{1, 2, 3}
	})
	defer patchHash.Unpatch()
	patchCol := monkey.PatchInstanceMethod(reflect.TypeOf(col), "Read", func(_ *Collection, id int) []byte {
		return []byte{'q'}
	})
	defer patchCol.Unpatch()

	d := defaultConfig()
	part := d.newPartition()
	res := part.ForEachDoc(0, 0, func(id int, doc []byte) bool {
		return false
	})
	if res != false {
		t.Error("Expected bool false")
	}
}
func TestApproxDocCountKeysEqualZero(t *testing.T) {
	var hash *HashTable
	patchHash := monkey.PatchInstanceMethod(reflect.TypeOf(hash), "GetPartition", func(_ *HashTable, partNum, partSize int) (keys, vals []int) {
		return []int{}, []int{1, 2, 3}
	})
	defer patchHash.Unpatch()
	d := defaultConfig()
	part := d.newPartition()
	if part.ApproxDocCount() != 0 {
		t.Error("Expected doc count digit zero")
	}
}

func TestClearError(t *testing.T) {
	os.Remove(tmp)
	defer os.Remove(tmp)
	var col *DataFile
	errMessage := "Error Clear"

	d := defaultConfig()
	part, _ := d.OpenPartition(tmp, tmp)
	patchCol := monkey.PatchInstanceMethod(reflect.TypeOf(col), "Clear", func(_ *DataFile) (err error) {
		return errors.New(errMessage)
	})
	defer patchCol.Unpatch()
	if part.Clear().Error() != string(dberr.ErrorIO) {
		t.Error("Expected error after call clear")
	}
}
func TestClose(t *testing.T) {
	os.Remove(tmp)
	defer os.Remove(tmp)
	var col *DataFile
	errMessage := "Error Close"

	d := defaultConfig()
	part, _ := d.OpenPartition(tmp, tmp)
	patchCol := monkey.PatchInstanceMethod(reflect.TypeOf(col), "Close", func(_ *DataFile) (err error) {
		return errors.New(errMessage)
	})
	defer patchCol.Unpatch()
	if part.Close().Error() != string(dberr.ErrorIO) {
		t.Error("Expected error after call close")
	}
}
