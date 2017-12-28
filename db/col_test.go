package db

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/HouzuoGuo/tiedot/data"
	"github.com/bouk/monkey"
	"github.com/pkg/errors"
)

func TestColMkDirErr(t *testing.T) {
	db, _ := OpenDB(TEST_DATA_DIR)
	errMessage := "Error mak dir"
	patch := monkey.Patch(os.MkdirAll, func(path string, perm os.FileMode) error {
		return errors.New(errMessage)
	})
	defer patch.Unpatch()
	_, err := OpenCol(db, "test")
	if err.Error() != errMessage {
		t.Error("Expected error message make dir")
	}
}
func TestOpenPartitionErr(t *testing.T) {
	db, _ := OpenDB(TEST_DATA_DIR)
	errMessage := "Error OpenPartition"
	patch := monkey.PatchInstanceMethod(reflect.TypeOf(db.Config), "OpenPartition", func(_ *data.Config, colPath, lookupPath string) (part *data.Partition, err error) {
		return nil, errors.New(errMessage)
	})
	defer patch.Unpatch()
	_, err := OpenCol(db, "test")
	if err.Error() != errMessage {
		t.Error("Expected error")
	}
}
func TestOpenReadDirErr(t *testing.T) {
	db, _ := OpenDB(TEST_DATA_DIR)
	errMessage := "Error read dir"
	patch := monkey.Patch(ioutil.ReadDir, func(dirname string) ([]os.FileInfo, error) {
		return nil, errors.New(errMessage)
	})
	defer patch.Unpatch()
	_, err := OpenCol(db, "test")
	if err.Error() != errMessage {
		t.Error("Expected error message")
	}
}
func TestLoadErrorOpenHashTableWhenParseIndex(t *testing.T) {
	defer os.RemoveAll(TEST_DATA_DIR)
	db, _ := OpenDB(TEST_DATA_DIR)
	index := "index_test"
	errMessage := "error OpenHashTable"
	col, _ := OpenCol(db, "test")
	col.Index([]string{index})
	for key, _ := range col.hts {
		col.hts[key] = nil
	}
	patch := monkey.PatchInstanceMethod(reflect.TypeOf(db.Config), "OpenHashTable", func(_ *data.Config, path string) (ht *data.HashTable, err error) {
		if strings.Contains(path, index) {
			return nil, errors.New(errMessage)
		}
		return
	})
	defer patch.Unpatch()

	if _, err := OpenCol(db, "test"); err.Error() != errMessage {
		t.Error("Expected error open hash table")
	}

}
func TestClose(t *testing.T) {
	defer os.RemoveAll(TEST_DATA_DIR)
	db, _ := OpenDB(TEST_DATA_DIR)
	col, _ := OpenCol(db, "test")
	col.Index([]string{"index"})

	errMessage := "Close error"
	var (
		datafile  *data.DataFile
		partition *data.Partition
	)

	patchDataFile := monkey.PatchInstanceMethod(reflect.TypeOf(datafile), "Close", func(_ *data.DataFile) (err error) {
		return errors.New(errMessage)
	})

	patchPartition := monkey.PatchInstanceMethod(reflect.TypeOf(partition), "Close", func(_ *data.Partition) (err error) {
		return nil
	})

	defer patchDataFile.Unpatch()
	defer patchPartition.Unpatch()
	if !strings.Contains(col.close().Error(), errMessage) {
		t.Error("Expected err message")
	}
}
func TestIndexMakeDirError(t *testing.T) {
	defer os.RemoveAll(TEST_DATA_DIR)
	db, _ := OpenDB(TEST_DATA_DIR)
	index := "index_test"
	errMessage := "error make dir"
	col, _ := OpenCol(db, "test")

	patch := monkey.Patch(os.MkdirAll, func(path string, perm os.FileMode) error {
		return errors.New(errMessage)
	})
	defer patch.Unpatch()

	col.Index([]string{index})
}
func TestIndexOpenHashTableError(t *testing.T) {
	defer os.RemoveAll(TEST_DATA_DIR)
	db, _ := OpenDB(TEST_DATA_DIR)
	index := "index_test"
	errMessage := "error open hash table"
	col, _ := OpenCol(db, "test")

	patch := monkey.PatchInstanceMethod(reflect.TypeOf(db.Config), "OpenHashTable", func(_ *data.Config, path string) (ht *data.HashTable, err error) {
		return nil, errors.New(errMessage)
	})
	defer patch.Unpatch()

	col.Index([]string{index})
}
func TestIndexErrorJsUnmarshal(t *testing.T) {
	defer os.RemoveAll(TEST_DATA_DIR)
	db, _ := OpenDB(TEST_DATA_DIR)
	index := "index_test"
	errMessage := "error json encoding"
	col, _ := OpenCol(db, "test")

	patch := monkey.Patch(json.Unmarshal, func(data []byte, v interface{}) error {
		return errors.New(errMessage)
	})
	defer patch.Unpatch()
	var part *data.Partition
	patchCol := monkey.PatchInstanceMethod(reflect.TypeOf(part), "ForEachDoc", func(_ *data.Partition, partNum, totalPart int, fun func(id int, doc []byte) bool) (moveOn bool) {
		fun(0, []byte{})
		return true
	})
	defer patchCol.Unpatch()
	col.Index([]string{index})
}
func TestUnindex(t *testing.T) {
	defer os.RemoveAll(TEST_DATA_DIR)
	db, _ := OpenDB(TEST_DATA_DIR)
	index := "index_test"
	errMessage := "error remove all"
	col, _ := OpenCol(db, "test")

	patch := monkey.Patch(os.RemoveAll, func(path string) error {
		return errors.New(errMessage)
	})
	defer patch.Unpatch()

	col.Index([]string{index})
	if col.Unindex([]string{index}).Error() != errMessage {
		t.Error("expected error remove all")
	}
}
func TestForEachDocInPageForEachDocIsFalse(t *testing.T) {
	defer os.RemoveAll(TEST_DATA_DIR)
	db, _ := OpenDB(TEST_DATA_DIR)

	errMessage := "error remove all"
	col, _ := OpenCol(db, "test")

	patch := monkey.Patch(os.RemoveAll, func(path string) error {
		return errors.New(errMessage)
	})
	defer patch.Unpatch()
	var part *data.Partition
	patchCol := monkey.PatchInstanceMethod(reflect.TypeOf(part), "ForEachDoc", func(_ *data.Partition, partNum, totalPart int, fun func(id int, doc []byte) bool) (moveOn bool) {
		return false
	})
	defer patchCol.Unpatch()
	col.ForEachDocInPage(0, 0, func(id int, doc []byte) bool {
		return true
	})
}
