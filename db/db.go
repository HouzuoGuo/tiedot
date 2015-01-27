/* Single shard - collection and DB storage management features. */
package db

import (
	"fmt"
	"github.com/HouzuoGuo/tiedot/tdlog"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"path"
	"path/filepath"
	"sort"
	"time"
)

// Single-shard database features.
type DB struct {
	// Root path of database data directory
	path string
	// Collection names VS collection structure
	cols map[string]*Col
}

// Open database and load all collections.
func OpenDB(dbPath string) (*DB, error) {
	rand.Seed(time.Now().UnixNano()) // initialize RNG for document ID generation
	db := &DB{path: dbPath}
	return db, db.load()
}

// Open collections and load the schema information.
func (db *DB) load() error {
	if err := os.MkdirAll(db.path, 0700); err != nil {
		return err
	}
	// Look for collection directories and open the collections
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
	sort.Strings(ret)
	return
}

// Return a reference to collection structure, which can be used to manage documents and collection schema.
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
	} else if err := db.cols[oldName].close(); err != nil {
		return err
	} else if err := os.Rename(path.Join(db.path, oldName), path.Join(db.path, newName)); err != nil {
		return err
	} else if db.cols[newName], err = OpenCol(db, newName); err != nil {
		return err
	}
	delete(db.cols, oldName)
	return nil
}

// Truncate a collection - delete all documents.
func (db *DB) Truncate(name string) error {
	if _, exists := db.cols[name]; !exists {
		return fmt.Errorf("Collection %s does not exist", name)
	}
	col := db.cols[name]
	if err := col.part.Clear(); err != nil {
		return err
	}
	for _, ht := range col.hts {
		if err := ht.Clear(); err != nil {
			return err
		}
	}
	return nil
}

// Drop a collection and lose all of its documents and indexes.
func (db *DB) Drop(name string) error {
	if _, exists := db.cols[name]; !exists {
		return fmt.Errorf("Collection %s does not exist", name)
	} else if err := db.cols[name].close(); err != nil {
		return err
	} else if err := os.RemoveAll(path.Join(db.path, name)); err != nil {
		return err
	}
	delete(db.cols, name)
	return nil
}

// Close all database files. This DB reference cannot be used any longer.
func (db *DB) Close() error {
	errs := make([]error, 0, 0)
	for _, col := range db.cols {
		if err := col.close(); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) == 0 {
		return nil
	}
	return fmt.Errorf("%v", errs)
}

// Copy this database into destination directory (for backup).
func (db *DB) Dump(dest string) error {
	cpFun := func(currPath string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			relPath, err := filepath.Rel(db.path, currPath)
			if err != nil {
				return err
			}
			destDir := path.Join(dest, relPath)
			if err := os.MkdirAll(destDir, 0700); err != nil {
				return err
			}
			tdlog.Noticef("Dump: created directory %s", destDir)
		} else {
			src, err := os.Open(currPath)
			if err != nil {
				return err
			}
			relPath, err := filepath.Rel(db.path, currPath)
			if err != nil {
				return err
			}
			destPath := path.Join(dest, relPath)
			if _, fileExists := os.Open(destPath); fileExists == nil {
				return fmt.Errorf("Destination file %s already exists", destPath)
			}
			destFile, err := os.Create(destPath)
			if err != nil {
				return err
			}
			written, err := io.Copy(destFile, src)
			if err != nil {
				return err
			}
			tdlog.Noticef("Dump: copied file %s, size is %d", destPath, written)
		}
		return nil
	}
	return filepath.Walk(db.path, cpFun)
}
