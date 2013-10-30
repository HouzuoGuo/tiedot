/* Collection management (Database is a collection of collections). */
package db

import (
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path"
	"strings"
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
				log.Printf("ERROR: Failed to open collection %s, reason: %v", f.Name(), err)
			} else {
				log.Printf("Successfully opened collection %s", f.Name())
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

// Repair (remove) damaged/deleted documents.
func (db *DB) Scrub(name string) (err error) {
	if col, ok := db.StrCol[name]; ok {
		db.Drop("scrub-" + name)
		if err = db.Create("scrub-" + name); err != nil {
			return
		}
		scrub := db.Use("scrub-" + name)
		if scrub == nil {
			return errors.New(fmt.Sprint("Scrub temporary collection has disappeared, please try again."))
		}
		// recreate indexes
		for path := range col.StrIC {
			// skip UID index, which has its config created automatically rather than manually
			if path[0] != '_' {
				if err = scrub.Index(strings.Split(path, ",")); err != nil {
					return
				}
			}
		}
		// recreate documents
		col.ForAll(func(id uint64, doc interface{}) bool {
			if _, err = scrub.Insert(doc); err != nil {
				log.Printf("ERROR: Scrubing %s, I could not insert '%v' back", name, doc)
			}
			return true
		})
		// replace original collection
		if err = db.Drop(name); err != nil {
			return
		}
		return db.Rename("scrub-"+name, name)
	} else {
		return errors.New(fmt.Sprintf("Collection %s does not exists in %s", name, db.Dir))
	}
	return nil
}

// Flush all collection data files.
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
