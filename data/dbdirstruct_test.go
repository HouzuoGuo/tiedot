package data

import (
	"io/ioutil"
	"os"
	"path"
	"testing"
)

func mustExist(path string, isDir bool, t *testing.T) {
	if stat, err := os.Stat(path); os.IsNotExist(err) {
		t.Fatal("mustExist", path, "does not exist")
	} else if err != nil {
		t.Fatal("mustExist", path, err)
	} else if stat.IsDir() != isDir {
		t.Fatal("mustExist", path, isDir)
	}
}

func mustNotExist(path string, t *testing.T) {
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("mustNotExist", path)
	}
}

func TestDBDirCrud(t *testing.T) {
	dir := "/tmp/tiedot_test_dbdir"
	os.RemoveAll(dir)
	defer os.RemoveAll(dir)

	// Open empty dir
	dirExists, latestVersion, err := DBIdentify(dir)
	if dirExists || latestVersion || err != nil {
		t.Fatal(dirExists, latestVersion, err)
	}

	// New DB dir
	if err := DBNewDir(dir, 2); err != nil {
		t.Fatal(err)
	}
	mustExist(path.Join(dir, VERSION_FILE), false, t)
	mustExist(path.Join(dir, NSHARDS_FILE), false, t)
	mustExist(path.Join(dir, "0", COLLECTION_DIR), true, t)
	mustExist(path.Join(dir, "1", COLLECTION_DIR), true, t)

	// Read dir
	dbfs, err := DBReadDir(dir)
	if err != nil {
		t.Fatal(err)
	} else if dbfs.Version != CURRENT_VERSION || dbfs.NShards != 2 || len(dbfs.Collections) != 0 || len(dbfs.Indexes) != 0 {
		t.Fatal(dbfs)
	}

	// It should not hurt to DBNew on an existing DB dir
	if err := DBNewDir(dir, 2); err != nil {
		t.Fatal(err)
	}

	// Open incompatible version
	if err := ioutil.WriteFile(path.Join(dir, VERSION_FILE), []byte("7890"), 0700); err != nil {
		t.Fatal(err)
	} else if reopen, err := DBReadDir(dir); err == nil {
		t.Fatal("did not err", reopen)
	} else if err := ioutil.WriteFile(path.Join(dir, VERSION_FILE), []byte(CURRENT_VERSION), 0700); err != nil { // restore
		t.Fatal(err)
	}

	// Collection CRUD
	if err := dbfs.CreateCollection("col2"); err != nil {
		t.Fatal(err)
	} else if err := dbfs.CreateCollection("col1"); err != nil {
		t.Fatal(err)
	} else if err := dbfs.CreateCollection("col1"); err == nil {
		t.Fatal("did not error")
	}
	allCols := dbfs.GetCollectionNamesSorted()
	if len(allCols) != 2 || allCols[0] != "col1" || allCols[1] != "col2" {
		t.Fatal(allCols)
	} else if err := dbfs.DropCollection("col2"); err != nil {
		t.Fatal(err)
	} else if err := dbfs.DropCollection("col2"); err == nil {
		t.Fatal("did not error")
	}
	allCols = dbfs.GetCollectionNamesSorted()
	if len(allCols) != 1 || allCols[0] != "col1" {
		t.Fatal(allCols)
	}
	mustExist(path.Join(dir, "0", COLLECTION_DIR, "col1", COLLECTION_DOC_DATA_FILE), false, t)
	mustExist(path.Join(dir, "1", COLLECTION_DIR, "col1", COLLECTION_DOC_DATA_FILE), false, t)
	mustExist(path.Join(dir, "0", COLLECTION_DIR, "col1", COLLECTION_ID_LOOKUP_FILE), false, t)
	mustExist(path.Join(dir, "1", COLLECTION_DIR, "col1", COLLECTION_ID_LOOKUP_FILE), false, t)
	mustExist(path.Join(dir, "0", COLLECTION_DIR, "col1", COLLECTION_INDEX_DIR), true, t)
	mustExist(path.Join(dir, "1", COLLECTION_DIR, "col1", COLLECTION_INDEX_DIR), true, t)
	mustNotExist(path.Join(dir, "0", COLLECTION_DIR, "col2"), t)
	mustNotExist(path.Join(dir, "1", COLLECTION_DIR, "col2"), t)

	// Index CRUD
	if err := dbfs.CreateIndex("col1", JoinIndexPath([]string{"b", "c"})); err != nil {
		t.Fatal(err)
	} else if err := dbfs.CreateIndex("col1", JoinIndexPath([]string{"a", "b", "c"})); err != nil {
		t.Fatal(err)
	} else if err := dbfs.CreateIndex("col1", JoinIndexPath([]string{"a", "b", "c"})); err == nil {
		t.Fatal("did not error")
	}
	allIndexes := dbfs.GetIndexesSorted("col1")
	if len(allIndexes) != 2 || allIndexes[0] != JoinIndexPath([]string{"a", "b", "c"}) || allIndexes[1] != JoinIndexPath([]string{"b", "c"}) {
		t.Fatal(allIndexes)
	} else if err := dbfs.DropIndex("col1", JoinIndexPath([]string{"b", "c", "d"})); err == nil {
		t.Fatal("did not error")
	} else if err := dbfs.DropIndex("col2", JoinIndexPath([]string{"b", "c"})); err == nil {
		t.Fatal("did not error")
	} else if err := dbfs.DropIndex("col1", JoinIndexPath([]string{"b", "c"})); err != nil {
		t.Fatal(err)
	}
	allIndexes = dbfs.GetIndexesSorted("col1")
	if len(allIndexes) != 1 || allIndexes[0] != JoinIndexPath([]string{"a", "b", "c"}) {
		t.Fatal(allIndexes)
	}

	// Reopen and verify paths
	dbfs, err = DBReadDir(dir)
	if err != nil {
		t.Fatal(err)
	} else if dbfs.Version != CURRENT_VERSION || dbfs.NShards != 2 || len(dbfs.Collections) != 1 || len(dbfs.Indexes) != 1 {
		t.Fatal(dbfs)
	}
	col1Data, col1ID := dbfs.GetCollectionDataFilePaths("col1", 0)
	if col1Data != path.Join(dir, "0", COLLECTION_DIR, "col1", COLLECTION_DOC_DATA_FILE) {
		t.Fatal(col1Data)
	}
	if col1ID != path.Join(dir, "0", COLLECTION_DIR, "col1", COLLECTION_ID_LOOKUP_FILE) {
		t.Fatal(col1ID)
	}
}
