package db

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/HouzuoGuo/tiedot/data"
	"github.com/bouk/monkey"
	"github.com/pkg/errors"
	"io/ioutil"
	"log"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"testing"
)

const (
	TEST_DATA_DIR = "/tmp/tiedot_test"
)

func touchFile(dir, filename string) {
	if err := os.MkdirAll(dir, 0700); err != nil {
		panic(err)
	}
	if err := ioutil.WriteFile(path.Join(dir, filename), make([]byte, 0), 0600); err != nil {
		panic(err)
	}
}

func TestOpenEmptyDB(t *testing.T) {
	os.RemoveAll(TEST_DATA_DIR)
	defer os.RemoveAll(TEST_DATA_DIR)
	db, err := OpenDB(TEST_DATA_DIR)
	if err != nil {
		t.Fatal(err)
	}
	if db.numParts != runtime.NumCPU() {
		t.Fatal(db.numParts)
	}
	if err := db.Create("a"); err != nil {
		t.Fatal(err)
	}
	if len(db.cols["a"].parts) != runtime.NumCPU() {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}
func TestOpenErrDB(t *testing.T) {
	os.RemoveAll(TEST_DATA_DIR)
	defer os.RemoveAll(TEST_DATA_DIR)
	if err := os.MkdirAll(TEST_DATA_DIR, 0700); err != nil {
		t.Fatal(err)
	}
	touchFile(TEST_DATA_DIR+"/ColA", "dat_0")
	touchFile(TEST_DATA_DIR+"/ColA/a!b!c", "0")
	if db, err := OpenDB(TEST_DATA_DIR); err == nil {
		t.Fatal("Did not error")
	} else if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}
func TestOpenCloseDB(t *testing.T) {
	os.RemoveAll(TEST_DATA_DIR)
	defer os.RemoveAll(TEST_DATA_DIR)
	if err := os.MkdirAll(TEST_DATA_DIR, 0700); err != nil {
		t.Fatal(err)
	}
	if err := ioutil.WriteFile(TEST_DATA_DIR+"/number_of_partitions", []byte("2"), 0600); err != nil {
		t.Fatal(err)
	}
	touchFile(TEST_DATA_DIR+"/ColA", "dat_0")
	touchFile(TEST_DATA_DIR+"/ColA/a!b!c", "0")
	if err := os.MkdirAll(TEST_DATA_DIR+"/ColB", 0700); err != nil {
		panic(err)
	}
	db, err := OpenDB(TEST_DATA_DIR)
	if err != nil {
		t.Fatal(err)
	}
	if db.path != TEST_DATA_DIR || db.numParts != 2 || db.cols["ColA"] == nil || db.cols["ColB"] == nil {
		t.Fatal(db.cols)
	}
	colA := db.cols["ColA"]
	colB := db.cols["ColB"]
	if len(colA.parts) != 2 || len(colA.hts) != 2 {
		t.Fatal(colA)
	}
	if colA.indexPaths["a!b!c"][0] != "a" || colA.indexPaths["a!b!c"][1] != "b" || colA.indexPaths["a!b!c"][2] != "c" {
		t.Fatal(colA.indexPaths)
	}
	if colA.hts[0]["a!b!c"] == nil || colA.hts[1]["a!b!c"] == nil {
		t.Fatal(colA.hts)
	}
	if len(colB.parts) != 2 || len(colB.hts) != 2 {
		t.Fatal(colB)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}
func TestColCrud(t *testing.T) {
	os.RemoveAll(TEST_DATA_DIR)
	defer os.RemoveAll(TEST_DATA_DIR)
	if err := os.MkdirAll(TEST_DATA_DIR, 0700); err != nil {
		t.Fatal(err)
	}
	if err := ioutil.WriteFile(TEST_DATA_DIR+"/number_of_partitions", []byte("2"), 0600); err != nil {
		t.Fatal(err)
	}
	db, err := OpenDB(TEST_DATA_DIR)
	if err != nil {
		t.Fatal(err)
	}
	if len(db.AllCols()) != 0 {
		t.Fatal(db.AllCols())
	}
	// Create
	if err := db.Create("a"); err != nil {
		t.Fatal(err)
	}
	if db.Create("a") == nil {
		t.Fatal("Did not error")
	}
	if err := db.Create("b"); err != nil {
		t.Fatal(err)
	}
	// Get all names & use
	if allNames := db.AllCols(); len(allNames) != 2 || !(allNames[0] == "a" && allNames[1] == "b" || allNames[0] == "b" && allNames[1] == "a") {
		t.Fatal(allNames)
	}
	if db.Use("a") == nil || db.Use("b") == nil || db.Use("abcde") != nil {
		t.Fatal(db.cols)
	}
	// Rename
	if db.Rename("a", "a") == nil {
		t.Fatal("Did not error")
	}
	if db.Rename("a", "b") == nil {
		t.Fatal("Did not error")
	}
	if db.Rename("abc", "b") == nil {
		t.Fatal("Did not error")
	}
	if err := db.Rename("a", "c"); err != nil {
		t.Fatal(err)
	}
	if err := db.Rename("b", "d"); err != nil {
		t.Fatal(err)
	}
	// Rename - verify
	if allNames := db.AllCols(); len(allNames) != 2 || !(allNames[0] == "d" && allNames[1] == "c" || allNames[0] == "c" && allNames[1] == "d") {
		t.Fatal(allNames)
	}
	if db.Use("c") == nil || db.Use("d") == nil || db.Use("a") != nil {
		t.Fatal(db.cols)
	}
	// Truncate
	if db.Truncate("a") == nil {
		t.Fatal("Did not error")
	}
	if err := db.Truncate("c"); err != nil {
		t.Fatal(err)
	}
	if err := db.Truncate("d"); err != nil {
		t.Fatal(err)
	}
	// Truncate - verify
	if allNames := db.AllCols(); len(allNames) != 2 || !(allNames[0] == "d" && allNames[1] == "c" || allNames[0] == "c" && allNames[1] == "d") {
		t.Fatal(allNames)
	}
	if db.Use("c") == nil || db.Use("d") == nil || db.Use("a") != nil {
		t.Fatal(db.cols)
	}
	// Scrub
	if err := db.Scrub("c"); err != nil {
		t.Fatal(err)
	}
	// Scrub - verify
	if allNames := db.AllCols(); len(allNames) != 2 || !(allNames[0] == "d" && allNames[1] == "c" || allNames[0] == "c" && allNames[1] == "d") {
		t.Fatal(allNames)
	}
	if db.Use("c") == nil || db.Use("d") == nil || db.Use("a") != nil {
		t.Fatal(db.cols)
	}
	// More scrub tests are in doc_test.go
	// Drop
	if db.Drop("a") == nil {
		t.Fatal("Did not error")
	}
	if err := db.Drop("c"); err != nil {
		t.Fatal(err)
	}
	if allNames := db.AllCols(); len(allNames) != 1 || allNames[0] != "d" {
		t.Fatal(allNames)
	}
	if db.Use("d") == nil {
		t.Fatal(db.cols)
	}
	if err := db.Drop("d"); err != nil {
		t.Fatal(err)
	}
	if allNames := db.AllCols(); len(allNames) != 0 {
		t.Fatal(allNames)
	}
	if db.Use("d") != nil {
		t.Fatal(db.cols)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	// ForceUse and ColExists
	if db.ForceUse("force-use-test") == nil {
		t.Fatal("did not return collection pointer")
	}
	if !db.ColExists("force-use-test") {
		t.Fatal("did not find newly created collection")
	}
	if db.ColExists("does-not-exist") {
		t.Fatal("did not identify non-existing collection")
	}
	if id, err := db.ForceUse("force-use-test").Insert(map[string]interface{}{"a": 1}); err != nil || id == 0 {
		t.Fatal(id, err)
	}
}
func TestDumpDB(t *testing.T) {
	var str bytes.Buffer
	log.SetOutput(&str)
	os.RemoveAll(TEST_DATA_DIR)
	os.RemoveAll(TEST_DATA_DIR + "bak")
	defer os.RemoveAll(TEST_DATA_DIR)
	defer os.RemoveAll(TEST_DATA_DIR + "bak")
	if err := os.MkdirAll(TEST_DATA_DIR, 0700); err != nil {
		t.Fatal(err)
	}
	if err := ioutil.WriteFile(TEST_DATA_DIR+"/number_of_partitions", []byte("2"), 0600); err != nil {
		t.Fatal(err)
	}
	db, err := OpenDB(TEST_DATA_DIR)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Create("a"); err != nil {
		t.Fatal(err)
	} else if err := db.Create("b"); err != nil {
		t.Fatal(err)
	}
	id1, err := db.Use("a").Insert(map[string]interface{}{"whatever": "1"})
	if err != nil {
		t.Fatal(err)
	} else if err := db.Dump(TEST_DATA_DIR + "bak"); err != nil {
		t.Fatal(err)
	}
	// Open the new database
	db2, err := OpenDB(TEST_DATA_DIR + "bak")
	if err != nil {
		t.Fatal(err)
	}
	if allCols := db2.AllCols(); !(allCols[0] == "a" && allCols[1] == "b" || allCols[0] == "b" && allCols[1] == "a") {
		t.Fatal(allCols)
	}
	if doc, err := db2.Use("a").Read(id1); err != nil || doc["whatever"].(string) != "1" {
		t.Fatal(doc, err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db2.Close(); err != nil {
		t.Fatal(err)
	}
}
func TestOpenErrorMDirAll(t *testing.T) {
	os.RemoveAll(TEST_DATA_DIR)
	defer os.RemoveAll(TEST_DATA_DIR)
	errMessage := "Make dir is unpossible"
	patch := monkey.Patch(os.MkdirAll, func(path string, perm os.FileMode) error {
		return errors.New(errMessage)
	})
	defer patch.Unpatch()
	if _, err := OpenDB(TEST_DATA_DIR); err.Error() != errMessage {
		t.Errorf("Expected error : '%s'", errMessage)
	}
}
func TestOpenErrorWriteInFilePartsNum(t *testing.T) {
	os.RemoveAll(TEST_DATA_DIR)
	defer os.RemoveAll(TEST_DATA_DIR)
	errMessage := "Write in file is unpossible"
	patch := monkey.Patch(ioutil.WriteFile, func(filename string, data []byte, perm os.FileMode) error {
		return errors.New(errMessage)
	})
	defer patch.Unpatch()
	if _, err := OpenDB(TEST_DATA_DIR); err.Error() != errMessage {
		t.Errorf("Expected error : '%s'", errMessage)
	}
}
func TestOpenNumPartsFilePathIsDir(t *testing.T) {
	os.RemoveAll(TEST_DATA_DIR)
	defer os.RemoveAll(TEST_DATA_DIR)
	touchFile(TEST_DATA_DIR+"/"+PART_NUM_FILE, "test")
	OpenDB(TEST_DATA_DIR)
}
func TestOpenErrWhenReadFileNumParts(t *testing.T) {
	os.RemoveAll(TEST_DATA_DIR)
	defer os.RemoveAll(TEST_DATA_DIR)
	errMessage := "Error read file"
	patch := monkey.Patch(ioutil.ReadFile, func(filename string) ([]byte, error) {
		return []byte{}, errors.New(errMessage)
	})
	defer patch.Unpatch()
	if _, err := OpenDB(TEST_DATA_DIR); err.Error() != errMessage {
		t.Errorf("Expected error : '%s'", errMessage)
	}
}
func TestOpenErrorValidFromFileNumParts(t *testing.T) {
	os.RemoveAll(TEST_DATA_DIR)
	defer os.RemoveAll(TEST_DATA_DIR)
	errMessage := "Error atoi"
	patch := monkey.Patch(strconv.Atoi, func(s string) (int, error) {
		return 0, errors.New(errMessage)
	})
	defer patch.Unpatch()
	if _, err := OpenDB(TEST_DATA_DIR); err.Error() != errMessage {
		t.Errorf("Expected error : '%s'", errMessage)
	}
}
func TestOpenErrorListDir(t *testing.T) {
	os.RemoveAll(TEST_DATA_DIR)
	defer os.RemoveAll(TEST_DATA_DIR)
	errMessage := "Error read dir"
	patch := monkey.Patch(ioutil.ReadDir, func(dirname string) ([]os.FileInfo, error) {
		return nil, errors.New(errMessage)
	})
	defer patch.Unpatch()
	if _, err := OpenDB(TEST_DATA_DIR); err.Error() != errMessage {
		t.Errorf("Expected error : '%s'", errMessage)
	}
}
func TestOpenColErr(t *testing.T) {
	os.RemoveAll(TEST_DATA_DIR)
	defer os.RemoveAll(TEST_DATA_DIR)
	errMessage := "Error open col"

	db, err := OpenDB(TEST_DATA_DIR)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Create("a"); err != nil {
		t.Fatal(err)
	} else if err := db.Create("b"); err != nil {
		t.Fatal(err)
	}
	patch := monkey.Patch(OpenCol, func(db *DB, name string) (*Col, error) {
		return nil, errors.New(errMessage)
	})
	defer patch.Unpatch()
	if _, err := OpenDB(TEST_DATA_DIR); err.Error() != errMessage {
		t.Errorf("Expected error : '%s'", errMessage)
	}
}
func TestCreateErrorMkDir(t *testing.T) {
	os.RemoveAll(TEST_DATA_DIR)
	defer os.RemoveAll(TEST_DATA_DIR)
	db, _ := OpenDB(TEST_DATA_DIR)
	errMessage := "Make dir is unpossible"
	patch := monkey.Patch(os.MkdirAll, func(path string, perm os.FileMode) error {
		return errors.New(errMessage)
	})
	defer patch.Unpatch()
	if db.Create("test").Error() != errMessage {
		t.Errorf("Expected error : '%s'", errMessage)
	}
}
func TestCreateErrOpenCol(t *testing.T) {
	os.RemoveAll(TEST_DATA_DIR)
	defer os.RemoveAll(TEST_DATA_DIR)
	errMessage := "Error open col"

	db, err := OpenDB(TEST_DATA_DIR)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Create("a"); err != nil {
		t.Fatal(err)
	} else if err := db.Create("b"); err != nil {
		t.Fatal(err)
	}
	patch := monkey.Patch(OpenCol, func(db *DB, name string) (*Col, error) {
		return nil, errors.New(errMessage)
	})
	defer patch.Unpatch()
	if db.Create("test").Error() != errMessage {
		t.Errorf("Expected error : '%s'", errMessage)
	}
}
func TestRenameCloseError(t *testing.T) {
	os.RemoveAll(TEST_DATA_DIR)
	defer os.RemoveAll(TEST_DATA_DIR)
	db, _ := OpenDB(TEST_DATA_DIR)
	col, _ := OpenCol(db, "test")
	db.cols = map[string]*Col{"test": col}
	var c *data.DataFile
	patch := monkey.PatchInstanceMethod(reflect.TypeOf(c), "Close", func(_ *data.DataFile) error {
		return errors.New("")
	})
	defer patch.Unpatch()
	db.Rename("test", "a")
}
func TestRenameOSErr(t *testing.T) {
	os.RemoveAll(TEST_DATA_DIR)
	defer os.RemoveAll(TEST_DATA_DIR)
	db, _ := OpenDB(TEST_DATA_DIR)
	col, _ := OpenCol(db, "test")
	db.cols = map[string]*Col{"test": col}
	errMessage := "Error rename file"
	patch := monkey.Patch(os.Rename, func(oldpath, newpath string) error {
		return errors.New(errMessage)
	})
	defer patch.Unpatch()
	if db.Rename("test", "a").Error() != errMessage {
		t.Errorf("Expected error : '%s'", errMessage)
	}

}
func TestRenameOpenColError(t *testing.T) {
	errMessage := "Error open col"
	os.RemoveAll(TEST_DATA_DIR)
	defer os.RemoveAll(TEST_DATA_DIR)
	db, _ := OpenDB(TEST_DATA_DIR)
	col, _ := OpenCol(db, "test")
	db.cols = map[string]*Col{"test": col}
	patch := monkey.Patch(OpenCol, func(db *DB, name string) (*Col, error) {
		return nil, errors.New(errMessage)
	})
	defer patch.Unpatch()
	if db.Rename("test", "a").Error() != errMessage {
		t.Errorf("Expected error : '%s'", errMessage)
	}
}
func TestTruncateColNotExist(t *testing.T) {
	os.RemoveAll(TEST_DATA_DIR)
	defer os.RemoveAll(TEST_DATA_DIR)
	db, _ := OpenDB(TEST_DATA_DIR)
	colName := "a"
	if db.Truncate(colName).Error() != fmt.Sprintf("Collection %s does not exist", colName) {
		t.Errorf("Expected error : collection not exist")
	}
}
func TestTruncatePartitionClear(t *testing.T) {
	os.RemoveAll(TEST_DATA_DIR)
	defer os.RemoveAll(TEST_DATA_DIR)
	db, err := OpenDB(TEST_DATA_DIR)
	col, _ := OpenCol(db, "test")
	db.cols = map[string]*Col{"test": col}
	col.parts = []*data.Partition{&data.Partition{}}

	if err != nil {
		t.Fatal(err)
	}
	if err := db.Create("a"); err != nil {
		t.Fatal(err)
	} else if err := db.Create("b"); err != nil {
		t.Fatal(err)
	}
	errMessage := "Error clear partition"
	var c *data.Partition
	monkey.PatchInstanceMethod(reflect.TypeOf(c), "Clear", func(_ *data.Partition) error {
		return errors.New(errMessage)
	})
	defer monkey.UnpatchInstanceMethod(reflect.TypeOf(c), "Clear")

	if db.Truncate("a").Error() != errMessage {
		t.Errorf("Expected error : '%s'", errMessage)
	}
}

func TestTruncateHashClearErr(t *testing.T) {
	os.RemoveAll(TEST_DATA_DIR)
	defer os.RemoveAll(TEST_DATA_DIR)
	errMessage := "Error clear hash"
	collectName := "test"
	db, _ := OpenDB(TEST_DATA_DIR)
	col, _ := OpenCol(db, collectName)
	db.cols = map[string]*Col{collectName: col}
	col.parts = []*data.Partition{&data.Partition{}}
	col.hts = []map[string]*data.HashTable{map[string]*data.HashTable{collectName: &data.HashTable{}}}

	var (
		hash *data.HashTable
		c    *data.Partition
	)
	monkey.PatchInstanceMethod(reflect.TypeOf(c), "Clear", func(_ *data.Partition) error {
		return nil
	})
	monkey.PatchInstanceMethod(reflect.TypeOf(hash), "Clear", func(_ *data.HashTable) error {
		return errors.New(errMessage)
	})
	defer monkey.UnpatchInstanceMethod(reflect.TypeOf(c), "Clear")
	defer monkey.UnpatchInstanceMethod(reflect.TypeOf(hash), "Clear")

	if db.Truncate(collectName).Error() != errMessage {
		t.Errorf("Expected error : '%s'", errMessage)
	}
}
func TestScrubCollectNotExist(t *testing.T) {
	os.RemoveAll(TEST_DATA_DIR)
	defer os.RemoveAll(TEST_DATA_DIR)
	db, _ := OpenDB(TEST_DATA_DIR)
	colName := "a"
	if db.Scrub(colName).Error() != fmt.Sprintf("Collection %s does not exist", colName) {
		t.Errorf("Expected error : collection not exist")
	}
}
func TestScrubMkDirAllWhichIndex(t *testing.T) {
	os.RemoveAll(TEST_DATA_DIR)
	defer os.RemoveAll(TEST_DATA_DIR)
	collectName := "test"
	errMessage := "Make dir is unpossible"

	db, _ := OpenDB(TEST_DATA_DIR)
	label := "label"
	db.cols = map[string]*Col{collectName: &Col{name: collectName, db: db, indexPaths: map[string][]string{collectName: []string{label}}}}

	patch := monkey.Patch(os.MkdirAll, func(path string, perm os.FileMode) error {
		if strings.Contains(path, label) {
			return errors.New(errMessage)
		}
		return nil
	})
	defer patch.Unpatch()
	if db.Scrub("test").Error() != errMessage {
		t.Errorf("Expected error : '%s'", errMessage)
	}
}
func TestScrubMDirAll(t *testing.T) {
	os.RemoveAll(TEST_DATA_DIR)
	defer os.RemoveAll(TEST_DATA_DIR)
	collectName := "test"
	errMessage := "Make dir is unpossible"

	db, _ := OpenDB(TEST_DATA_DIR)
	db.cols = map[string]*Col{collectName: &Col{name: collectName, db: db, indexPaths: map[string][]string{collectName: []string{}}}}

	patch := monkey.Patch(os.MkdirAll, func(path string, perm os.FileMode) error {
		return errors.New(errMessage)
	})
	defer patch.Unpatch()

	if db.Scrub(collectName).Error() != errMessage {
		t.Errorf("Expected error : '%s'", errMessage)
	}
}
func TestScrubErrOpenCol(t *testing.T) {
	os.RemoveAll(TEST_DATA_DIR)
	defer os.RemoveAll(TEST_DATA_DIR)
	collectName := "test"
	errMessage := "Error open col"
	db, _ := OpenDB(TEST_DATA_DIR)
	col, _ := OpenCol(db, collectName)
	db.cols = map[string]*Col{collectName: col}
	patch := monkey.Patch(OpenCol, func(db *DB, name string) (*Col, error) {
		return nil, errors.New(errMessage)
	})
	defer patch.Unpatch()

	if db.Scrub(collectName).Error() != errMessage {
		t.Errorf("Expected error : '%s'", errMessage)
	}
}

func TestScrubCorrupted(t *testing.T) {
	os.RemoveAll(TEST_DATA_DIR)
	defer os.RemoveAll(TEST_DATA_DIR)
	collectName := "test"
	database, _ := OpenDB(TEST_DATA_DIR)
	col, _ := OpenCol(database, collectName)
	database.cols = map[string]*Col{collectName: col}

	var Obj *data.Partition
	patch := monkey.PatchInstanceMethod(reflect.TypeOf(Obj), "ForEachDoc", func(_ *data.Partition, partNum, totalPart int, fun func(id int, doc []byte) bool) (moveOn bool) {
		fun(0, []byte{})
		return
	})
	defer patch.Unpatch()
	database.Scrub(collectName)
}
func TestScrubInsertRecoveryErr(t *testing.T) {
	os.RemoveAll(TEST_DATA_DIR)
	defer os.RemoveAll(TEST_DATA_DIR)
	collectName := "test"
	errMessage := "Error InsertRecovery"
	database, _ := OpenDB(TEST_DATA_DIR)
	col, _ := OpenCol(database, collectName)
	database.cols = map[string]*Col{collectName: col}

	var (
		Obj    *data.Partition
		ObjCol *Col
		str    bytes.Buffer
	)
	log.SetOutput(&str)
	objPatch := monkey.PatchInstanceMethod(reflect.TypeOf(Obj), "ForEachDoc", func(_ *data.Partition, partNum, totalPart int, fun func(id int, doc []byte) bool) (moveOn bool) {
		fun(0, []byte{})
		return
	})
	objColPatch := monkey.PatchInstanceMethod(reflect.TypeOf(ObjCol), "InsertRecovery", func(_ *Col, id int, doc map[string]interface{}) (err error) {
		return errors.New(errMessage)
	})

	patch := monkey.Patch(json.Unmarshal, func(data []byte, v interface{}) error {
		return nil
	})
	defer patch.Unpatch()
	defer objPatch.Unpatch()
	defer objColPatch.Unpatch()

	database.Scrub(collectName)
	if !strings.Contains(str.String(), "Scrub test: failed to insert back document map") {
		t.Error("Expected failed insert back document map[]")
	}
}
func TestScrubErrRemoveAll(t *testing.T) {
	os.RemoveAll(TEST_DATA_DIR)
	defer os.RemoveAll(TEST_DATA_DIR)
	collectName := "test"
	errMessage := "Remove error"
	database, _ := OpenDB(TEST_DATA_DIR)
	col, _ := OpenCol(database, collectName)
	database.cols = map[string]*Col{collectName: col}

	patch := monkey.Patch(os.RemoveAll, func(path string) error {
		return errors.New(errMessage)
	})
	defer patch.Unpatch()

	if database.Scrub(collectName).Error() != errMessage {
		t.Errorf("Expected error : '%s'", errMessage)
	}
}
func TestScrubErrRename(t *testing.T) {
	os.RemoveAll(TEST_DATA_DIR)
	defer os.RemoveAll(TEST_DATA_DIR)
	collectName := "test"
	errMessage := "Rename error"
	database, _ := OpenDB(TEST_DATA_DIR)
	col, _ := OpenCol(database, collectName)
	database.cols = map[string]*Col{collectName: col}

	patch := monkey.Patch(os.Rename, func(oldpath, newpath string) error {
		return errors.New(errMessage)
	})
	defer patch.Unpatch()

	if database.Scrub(collectName).Error() != errMessage {
		t.Errorf("Expected error : '%s'", errMessage)
	}
}
func TestScrubTmpColClose(t *testing.T) {
	os.RemoveAll(TEST_DATA_DIR)
	defer os.RemoveAll(TEST_DATA_DIR)
	collectName := "test"
	errMessage := "tmp close error"
	database, _ := OpenDB(TEST_DATA_DIR)
	col, _ := OpenCol(database, collectName)
	database.cols = map[string]*Col{collectName: col}

	var (
		dataFile *data.DataFile
		str      bytes.Buffer
	)
	log.SetOutput(&str)
	objPatch := monkey.PatchInstanceMethod(reflect.TypeOf(dataFile), "Close", func(_ *data.DataFile) (err error) {
		return errors.New(errMessage)
	})
	defer objPatch.Unpatch()
	database.Scrub(collectName)
}
func TestDropColNotExist(t *testing.T) {
	os.RemoveAll(TEST_DATA_DIR)
	defer os.RemoveAll(TEST_DATA_DIR)
	db, _ := OpenDB(TEST_DATA_DIR)
	colName := "a"
	if db.Drop(colName).Error() != fmt.Sprintf("Collection %s does not exist", colName) {
		t.Errorf("Expected error : collection not exist")
	}
}
func TestDropErrRemoveAll(t *testing.T) {
	os.RemoveAll(TEST_DATA_DIR)
	defer os.RemoveAll(TEST_DATA_DIR)
	collectName := "test"
	errMessage := "Remove error"
	database, _ := OpenDB(TEST_DATA_DIR)
	col, _ := OpenCol(database, collectName)
	database.cols = map[string]*Col{collectName: col}

	patch := monkey.Patch(os.RemoveAll, func(path string) error {
		return errors.New(errMessage)
	})
	defer patch.Unpatch()

	if database.Drop(collectName).Error() != errMessage {
		t.Errorf("Expected error : '%s'", errMessage)
	}
}
func TestDropCloseColErr(t *testing.T) {
	os.RemoveAll(TEST_DATA_DIR)
	defer os.RemoveAll(TEST_DATA_DIR)
	collectName := "test"
	errMessage := "error"
	database, _ := OpenDB(TEST_DATA_DIR)
	col, _ := OpenCol(database, collectName)
	database.cols = map[string]*Col{collectName: col}
	var (
		dataFile *data.DataFile
		str      bytes.Buffer
	)
	log.SetOutput(&str)
	objPatch := monkey.PatchInstanceMethod(reflect.TypeOf(dataFile), "Close", func(_ *data.DataFile) (err error) {
		return errors.New(errMessage)
	})
	defer objPatch.Unpatch()
	patch := monkey.Patch(os.RemoveAll, func(path string) error {
		return errors.New(errMessage)
	})
	defer patch.Unpatch()
	database.Drop(collectName)
}
func TestCloseErrorCloseCol(t *testing.T) {
	os.RemoveAll(TEST_DATA_DIR)
	defer os.RemoveAll(TEST_DATA_DIR)
	errMessage := "Close error"
	database, _ := OpenDB(TEST_DATA_DIR)
	database.Create("test")

	var part *data.Partition
	patchCol := monkey.PatchInstanceMethod(reflect.TypeOf(part), "Close", func(_ *data.Partition) error {
		return errors.New(errMessage)
	})
	defer patchCol.Unpatch()

	if !strings.Contains(database.Close().Error(), errMessage) {
		t.Error("Expexcted error message close db")
	}

}
func TestDumpRelErr(t *testing.T) {
	os.RemoveAll(TEST_DATA_DIR)
	defer os.RemoveAll(TEST_DATA_DIR)
	database, _ := OpenDB(TEST_DATA_DIR)
	database.Create("test")

	errMessage := "error rel return"
	patch := monkey.Patch(filepath.Rel, func(basepath, targpath string) (string, error) {
		return "", errors.New(errMessage)
	})
	defer patch.Unpatch()

	if database.Dump("test").Error() != errMessage {
		t.Error("Expected error message rel")
	}
}
func TestDumpMkDirErr(t *testing.T) {
	os.RemoveAll(TEST_DATA_DIR)
	defer os.RemoveAll(TEST_DATA_DIR)
	database, _ := OpenDB(TEST_DATA_DIR)
	database.Create("test")

	errMessage := "error make dir"
	patch := monkey.Patch(os.MkdirAll, func(path string, perm os.FileMode) error {
		return errors.New(errMessage)
	})
	defer patch.Unpatch()

	if database.Dump("test").Error() != errMessage {
		t.Error("Expected error make dir error")
	}
}
