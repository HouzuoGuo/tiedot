/* Database is a collection of collections. */
package db

import (
	"errors"
	"fmt"
	"github.com/HouzuoGuo/tiedot/tdlog"
	"io/ioutil"
	"os"
	"path"
	"strconv"
)

const (
	NUMCHUNKS_FILENAME = "numchunks"
)

type DB struct {
	BaseDir string          // Database directory path
	StrCol  map[string]*Col // Collection name to collection mapping
}

func OpenDB(baseDir string) (db *DB, err error) {
	if err = os.MkdirAll(baseDir, 0700); err != nil {
		return
	}
	db = &DB{BaseDir: baseDir, StrCol: make(map[string]*Col)}
	files, err := ioutil.ReadDir(baseDir)
	if err != nil {
		return
	}
	// Try to open sub-directory as document collection
	for _, f := range files {
		if f.IsDir() {
			// Figure out how many chunks there are in the collection
			var numchunksFH *os.File
			numchunksFH, err = os.OpenFile(path.Join(baseDir, f.Name(), NUMCHUNKS_FILENAME), os.O_CREATE|os.O_RDWR, 0600)
			defer numchunksFH.Close()
			if err != nil {
				return
			}
			numchunksContent, err := ioutil.ReadAll(numchunksFH)
			if err != nil {
				panic(err)
			}
			numchunks, err := strconv.Atoi(string(numchunksContent))
			if err != nil {
				panic(fmt.Sprintf("Cannot figure out number of chunks for collection %s, manually repair it maybe? %v", baseDir, err))
			}

			// Open the directory as a collection
			if db.StrCol[f.Name()], err = OpenCol(path.Join(baseDir, f.Name()), uint64(numchunks)); err != nil {
				tdlog.Errorf("ERROR: Failed to open collection %s, error: %v", f.Name(), err)
			} else {
				tdlog.Printf("Successfully opened collection %s", f.Name())
			}
		}
	}
	return
}

// Create a collection.
func (db *DB) Create(name string, numChunks uint64) (err error) {
	if _, nope := db.StrCol[name]; nope {
		return errors.New(fmt.Sprintf("Collection %s already exists in %s", name, db.BaseDir))
	}
	if db.StrCol[name], err = OpenCol(path.Join(db.BaseDir, name), numChunks); err != nil {
		return
	}
	if err = ioutil.WriteFile(path.Join(db.BaseDir, name, NUMCHUNKS_FILENAME), []byte(fmt.Sprint(numChunks)), 0600); err != nil {
		return
	}
	return err
}

// Return collection reference by collection name. This function is safe for concurrent use.
func (db *DB) Use(name string) *Col {
	if col, ok := db.StrCol[name]; ok {
		return col
	}
	return nil
}

// Rename a collection.
func (db *DB) Rename(oldName, newName string) (err error) {
	var numChunks uint64
	if col, ok := db.StrCol[oldName]; ok {
		numChunks = uint64(len(db.StrCol[oldName].Chunks))
		col.Close()
	} else {
		return errors.New(fmt.Sprintf("Collection %s does not exists in %s", oldName, db.BaseDir))
	}
	if _, nope := db.StrCol[newName]; nope {
		return errors.New(fmt.Sprintf("Collection name %s is already used in %s", newName, db.BaseDir))
	}
	delete(db.StrCol, oldName)
	if err = os.Rename(path.Join(db.BaseDir, oldName), path.Join(db.BaseDir, newName)); err != nil {
		return
	}
	db.StrCol[newName], err = OpenCol(path.Join(db.BaseDir, newName), numChunks)
	return
}

// Drop (delete) a collection.
func (db *DB) Drop(name string) (err error) {
	if col, ok := db.StrCol[name]; ok {
		col.Close()
		delete(db.StrCol, name)
		return os.RemoveAll(path.Join(db.BaseDir, name))
	} else {
		return errors.New(fmt.Sprintf("Collection %s does not exists in %s", name, db.BaseDir))
	}
}

// Flush all collection data and index files.
func (db *DB) Flush() {
	for _, col := range db.StrCol {
		if err := col.Flush(); err != nil {
			tdlog.Errorf("Error during database flush: %v", err)
		}
	}
}

// Close all collections.
func (db *DB) Close() {
	for _, col := range db.StrCol {
		col.Close()
	}
}
