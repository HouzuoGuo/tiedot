/* Collection management (Database is a collection of collections). */
package db

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path"
)

type DB struct {
	Dir    string
	StrCol map[string]*Col
}

func OpenDB(dir string) (db *DB, err error) {
	if err = os.MkdirAll(dir, 0700); err != nil {
		return
	}
	db = &DB{Dir: dir, StrCol: make(map[string]*Col)}
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return
	}
	for _, f := range files {
		if f.IsDir() {
			if db.StrCol[f.Name()], err = OpenCol(path.Join(dir, f.Name())); err != nil {
				fmt.Fprintf(os.Stderr, "Failed to open collection %s, reason: %v\n", f.Name(), err)
			} else {
				fmt.Fprintf(os.Stderr, "Successfully opened collection %s\n", f.Name(), err)
			}
		}
	}
	return
}

// Create a new collection.
func (db *DB) Create(name string) (err error) {
	if _, nope := db.StrCol[name]; nope {
		return errors.New(fmt.Sprintf("Collection %s already exists in %s", name, db.Dir))
	}
	db.StrCol[name], err = OpenCol(path.Join(db.Dir, name))
	return err
}

// Return collection reference.
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
		return errors.New(fmt.Sprintf("Collection %s does not exists in %s", oldName, db.Dir))
	}
	if _, nope := db.StrCol[newName]; nope {
		return errors.New(fmt.Sprintf("Collection name %s is already used in %s", newName, db.Dir))
	}
	delete(db.StrCol, oldName)
	if err = os.Rename(path.Join(db.Dir, oldName), path.Join(db.Dir, newName)); err != nil {
		return
	}
	db.StrCol[newName], err = OpenCol(path.Join(db.Dir, newName))
	return
}

// Drop (delete) a collection.
func (db *DB) Drop(name string) (err error) {
	if col, ok := db.StrCol[name]; ok {
		col.Close()
		delete(db.StrCol, name)
		return os.RemoveAll(path.Join(db.Dir, name))
	} else {
		return errors.New(fmt.Sprintf("Collection %s does not exists in %s", name, db.Dir))
	}
}

// Close all collections.
func (db *DB) Close() {
	for _, col := range db.StrCol {
		col.Close()
	}
}
