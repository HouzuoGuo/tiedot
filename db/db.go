/* Database is a collection of collections. */
package db

import (
	"errors"
	"fmt"
	"github.com/HouzuoGuo/tiedot/tdlog"
	"io/ioutil"
	"os"
	"path"
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
			if db.StrCol[f.Name()], err = OpenCol(path.Join(baseDir, f.Name())); err != nil {
				tdlog.Errorf("ERROR: Failed to open collection %s, error: %v", f.Name(), err)
			} else {
				tdlog.Printf("Successfully opened collection %s", f.Name())
			}
		}
	}
	return
}

// Create a collection.
func (db *DB) Create(name string) (err error) {
	if _, nope := db.StrCol[name]; nope {
		return errors.New(fmt.Sprintf("Collection %s already exists in %s", name, db.BaseDir))
	}
	db.StrCol[name], err = OpenCol(path.Join(db.BaseDir, name))
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
	if col, ok := db.StrCol[oldName]; ok {
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
	db.StrCol[newName], err = OpenCol(path.Join(db.BaseDir, newName))
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
		col.Flush()
	}
}

// Close all collections.
func (db *DB) Close() {
	for _, col := range db.StrCol {
		col.Close()
	}
}
