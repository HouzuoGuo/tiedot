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
	"time"
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
			if db.StrCol[f.Name()], err = OpenCol(path.Join(baseDir, f.Name()), numchunks); err != nil {
				tdlog.Errorf("ERROR: Failed to open collection %s, error: %v", f.Name(), err)
			} else {
				tdlog.Printf("Successfully opened collection %s", f.Name())
			}
		}
	}
	return
}

// Create a collection.
func (db *DB) Create(name string, numChunks int) (err error) {
	if numChunks < 1 {
		return errors.New(fmt.Sprintf("Number of of partitions must be above 0 - failed to open %s", numChunks, name))
	}
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
	var numChunks int
	col, ok := db.StrCol[oldName]
	if ok {
		numChunks = len(db.StrCol[oldName].Chunks)
	} else {
		return errors.New(fmt.Sprintf("Collection %s does not exists in %s", oldName, db.BaseDir))
	}
	if _, nope := db.StrCol[newName]; nope {
		return errors.New(fmt.Sprintf("Collection name %s is already used in %s", newName, db.BaseDir))
	}
	col.Close()
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

// Compact and repair a collection.
func (db *DB) Scrub(name string) (counter uint64, err error) {
	target := db.Use(name)
	if target == nil {
		return 0, errors.New(fmt.Sprintf("Collection %s does not exist in %s", name, db.BaseDir))
	}
	// Create a temporary collection
	numChunks := target.NumChunks
	tempName := fmt.Sprintf("temp-%s-%v", name, time.Now().Unix())
	db.Create(tempName, numChunks)
	temp := db.Use(tempName)
	// Recreate secondary indexes
	for _, index := range target.SecIndexes {
		temp.Index(index[0].Path)
	}
	// Reinsert documents
	target.ForAll(func(id int, doc map[string]interface{}) bool {
		if err := temp.InsertRecovery(id, doc); err == nil {
			counter += 1
		} else {
			tdlog.Errorf("Failed to recover document %v", doc)
		}
		return true
	})
	// Drop the old collection and rename the recovery collection
	if err = db.Drop(name); err != nil {
		tdlog.Errorf("Scrub operation failed to drop original collection %s: %v", name, err)
		return
	}
	if err = db.Rename(tempName, name); err != nil {
		tdlog.Errorf("Scrub operation failed to rename recovery collection %s: %v", tempName, err)
	}
	return
}

// Change the number of partitions in collection
func (db *DB) Repartition(name string, newNumber int) (counter uint64, err error) {
	target := db.Use(name)
	if target == nil {
		return 0, errors.New(fmt.Sprintf("Collection %s does not exist in %s", name, db.BaseDir))
	}
	if newNumber < 1 {
		return 0, errors.New(fmt.Sprintf("New number of partitions must be above 0, %d given", newNumber))
	}
	// Create a temporary collection
	tempName := fmt.Sprintf("temp-%s-%v", name, time.Now().Unix())
	db.Create(tempName, newNumber)
	temp := db.Use(tempName)
	// Recreate secondary indexes
	for _, index := range target.SecIndexes {
		temp.Index(index[0].Path)
	}
	// Reinsert documents
	target.ForAll(func(id int, doc map[string]interface{}) bool {
		if err := temp.InsertRecovery(id, doc); err == nil {
			counter += 1
		} else {
			tdlog.Errorf("Failed to recover document %v", doc)
		}
		return true
	})
	// Drop the old collection and rename the recovery collection
	if err = db.Drop(name); err != nil {
		tdlog.Errorf("Scrub operation failed to drop original collection %s: %v", name, err)
		return
	}
	if err = db.Rename(tempName, name); err != nil {
		tdlog.Errorf("Scrub operation failed to rename recovery collection %s: %v", tempName, err)
	}
	return
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
