package db

import (
	"testing"
	"github.com/bouk/monkey"
	"os"
	"github.com/pkg/errors"
	"github.com/HouzuoGuo/tiedot/data"
	"io/ioutil"
)
func TestColMkDirErr(t *testing.T) {
	db, _ := OpenDB(TEST_DATA_DIR)
	errMessage := "Error mak dir"
	patch := monkey.Patch(os.MkdirAll, func(path string, perm os.FileMode) error  {
		return errors.New(errMessage)
	})
	defer patch.Unpatch()
	_, err := OpenCol(db, "test")
	if 	err.Error() != errMessage {
		t.Error("Expected error message make dir")
	}
}
func TestOpenPartitionErr(t *testing.T) {
	db, _ := OpenDB(TEST_DATA_DIR)
	errMessage := "Error OpenPartition"
	patch := monkey.Patch(data.OpenPartition, func(colPath, lookupPath string) (part *data.Partition, err error)  {
		return nil, errors.New(errMessage)
	})
	defer patch.Unpatch()
	_, err := OpenCol(db, "test")
	if 	err.Error() != errMessage {
		t.Error("Expected error")
	}
}
func TestOpenReadDirErr(t *testing.T) {
	db, _ := OpenDB(TEST_DATA_DIR)
	errMessage := "Error read dir"
	patch := monkey.Patch(ioutil.ReadDir, func(dirname string) ([]os.FileInfo, error)   {
		return nil, errors.New(errMessage)
	})
	defer patch.Unpatch()
	_, err := OpenCol(db, "test")
	if 	err.Error() != errMessage {
		t.Error("Expected error message")
	}
}