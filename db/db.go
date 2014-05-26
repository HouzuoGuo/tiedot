// Database logic.
package db

import (
	"encoding/json"
	"fmt"
	"github.com/HouzuoGuo/tiedot/tdlog"
	"io/ioutil"
	"os"
	"path"
	"strconv"
	"strings"
	"time"
)

const (
	PART_NUM_FILE = "number_of_partitions"
)

// Database structures.
type DB struct {
	path     string          // root path of database directory
	numParts int             // total number of partitions
	cols     map[string]*Col // collection by name lookup
}

// Open database and load all collections & indexes.
func OpenDB(dbPath string) (*DB, error) {
	db := &DB{path: dbPath}
	return db, db.load()
}

// Load all collection schema.
func (db *DB) load() error {
	// Get number of partitions from the text file
	if numParts, err := ioutil.ReadFile(path.Join(db.path, PART_NUM_FILE)); err != nil {
		return err
	} else if db.numParts, err = strconv.Atoi(string(numParts)); err != nil {
		return err
	}
	// Look for collection directories
	db.cols = make(map[string]*Col)
	dirContent, err := ioutil.ReadDir(db.path)
	if err != nil {
		return err
	}
	for _, maybeColDir := range dirContent {
		if !maybeColDir.IsDir() {
			continue
		}
		if db.cols[maybeColDir.Name()], err = OpenCol(db, maybeColDir.Name()); err != nil {
			return err
		}
	}
	return err
}

// Synchronize all data files to disk.
func (db *DB) Sync() error {
	errs := make([]error, 0, 0)
	for _, col := range db.cols {
		if err := col.Sync(); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) == 0 {
		return nil
	}
	return fmt.Errorf("%v", errs)
}

// Close all database files. Do not use the DB afterwards!
func (db *DB) Close() error {
	errs := make([]error, 0, 0)
	for _, col := range db.cols {
		if err := col.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) == 0 {
		return nil
	}
	return fmt.Errorf("%v", errs)
}

// Create a new collection.
func (db *DB) Create(name string) error {
	if _, exists := db.cols[name]; exists {
		return fmt.Errorf("Collection %s already exists", name)
	} else if err := os.MkdirAll(path.Join(db.path, name), 0700); err != nil {
		return err
	} else if db.cols[name], err = OpenCol(db, name); err != nil {
		return err
	}
	return nil
}

// Return all collection names.
func (db *DB) AllCols() (ret []string) {
	ret = make([]string, 0, len(db.cols))
	for name, _ := range db.cols {
		ret = append(ret, name)
	}
	return
}

// Use the return value to interact with collection. Return value may be nil if the collection does not exist.
func (db *DB) Use(name string) *Col {
	if col, exists := db.cols[name]; exists {
		return col
	}
	return nil
}

// Rename a collection.
func (db *DB) Rename(oldName, newName string) error {
	if _, exists := db.cols[oldName]; !exists {
		return fmt.Errorf("Collection %s does not exist", oldName)
	} else if _, exists := db.cols[newName]; exists {
		return fmt.Errorf("Collection %s already exists", newName)
	} else if newName == oldName {
		return fmt.Errorf("Old and new names are the same")
	} else if err := db.cols[oldName].Close(); err != nil {
		return err
	} else if err := os.Rename(path.Join(db.path, oldName), path.Join(db.path, newName)); err != nil {
		return err
	} else if db.cols[newName], err = OpenCol(db, newName); err != nil {
		return err
	}
	delete(db.cols, oldName)
	return nil
}

// Truncate a collection - delete all documents and clear
func (db *DB) Truncate(name string) error {
	if _, exists := db.cols[name]; !exists {
		return fmt.Errorf("Collection %s does not exist", name)
	} else if err := db.cols[name].Sync(); err != nil {
		return err
	}
	col := db.cols[name]
	for i := 0; i < db.numParts; i++ {
		if err := col.parts[i].Clear(); err != nil {
			return err
		}
		for _, ht := range col.hts[i] {
			if err := ht.Clear(); err != nil {
				return err
			}
		}
	}
	return nil
}

// Scrub a collection - fix corrupted documents and de-fragment free space.
func (db *DB) Scrub(name string) error {
	if _, exists := db.cols[name]; !exists {
		return fmt.Errorf("Collection %s does not exist", name)
	} else if err := db.cols[name].Sync(); err != nil {
		return err
	}
	// Prepare a temporary collection in file system
	tmpColName := fmt.Sprintf("scrub-%s-%d", name, time.Now().UnixNano())
	tmpColDir := path.Join(db.path, tmpColName)
	if err := os.MkdirAll(tmpColDir, 0700); err != nil {
		return err
	}
	// Mirror indexes from original collection
	for _, idxPath := range db.cols[name].indexPaths {
		if err := os.MkdirAll(path.Join(tmpColDir, strings.Join(idxPath, INDEX_PATH_SEP)), 0700); err != nil {
			return err
		}
	}
	// Iterate through all documents and put them into the temporary collection
	tmpCol, err := OpenCol(db, tmpColName)
	if err != nil {
		return err
	}
	db.cols[name].ForEachDoc(false, func(id int, doc []byte) (moveOn bool) {
		var docObj map[string]interface{}
		if err := json.Unmarshal([]byte(doc), &docObj); err != nil {
			// Skip corrupted document
			return true
		}
		if err := tmpCol.InsertRecovery(id, docObj); err != nil {
			tdlog.Errorf("Scrub %s: failed to insert back document %v", name, docObj)
		}
		return true
	})
	if err := tmpCol.Close(); err != nil {
		return err
	}
	// Replace the original collection with the "temporary" one
	db.cols[name].Close()
	if err := os.RemoveAll(path.Join(db.path, name)); err != nil {
		return err
	}
	if err := os.Rename(path.Join(db.path, tmpColName), path.Join(db.path, name)); err != nil {
		return err
	}
	if db.cols[name], err = OpenCol(db, name); err != nil {
		return err
	}
	return nil
}

// Drop a collection and lose all of its documents and indexes.
func (db *DB) Drop(name string) error {
	if _, exists := db.cols[name]; !exists {
		return fmt.Errorf("Collection %s does not exist", name)
	} else if err := db.cols[name].Close(); err != nil {
		return err
	} else if err := os.RemoveAll(path.Join(db.path, name)); err != nil {
		return err
	}
	delete(db.cols, name)
	return nil
}
