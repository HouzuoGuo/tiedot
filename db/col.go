/* Single shard - collection schema and index management features. */
package db

import (
	"fmt"
	"github.com/HouzuoGuo/tiedot/data"
	"io/ioutil"
	"os"
	"path"
	"sort"
	"strings"
)

const (
	// Name of collection data file
	DOC_DATA_FILE = "dat"
	// Name of document ID lookup hash table
	DOC_LOOKUP_FILE = "id"
	// Index path separator (appears in hash table file name)
	INDEX_PATH_SEP = "!"
)

// Schema information and document/index management features for a single DB shard.
type Col struct {
	db   *DB
	name string
	// Document data
	part *data.Partition
	// Joint index paths VS hash table
	hts map[string]*data.HashTable
	// Joint index paths VS split index paths
	indexPaths map[string][]string
	// (For multi-shard environment) currently held document locks
	locked map[uint64]struct{}
}

// Open a collection and load schema information.
func OpenCol(db *DB, name string) (*Col, error) {
	col := &Col{db: db, name: name, locked: make(map[uint64]struct{})}
	if err := os.MkdirAll(path.Join(col.db.path, col.name), 0700); err != nil {
		return nil, err
	}
	return col, col.load()
}

// Load collection schema (index schema).
func (col *Col) load() error {
	col.hts = make(map[string]*data.HashTable)
	col.indexPaths = make(map[string][]string)
	// Open document collection
	var err error
	if col.part, err = data.OpenPartition(
		path.Join(col.db.path, col.name, DOC_DATA_FILE),
		path.Join(col.db.path, col.name, DOC_LOOKUP_FILE)); err != nil {
		return err
	}
	// Search for index files
	colDirContent, err := ioutil.ReadDir(path.Join(col.db.path, col.name))
	if err != nil {
		return err
	}
	for _, htFile := range colDirContent {
		if htFile.IsDir() || htFile.Name() == DOC_DATA_FILE || htFile.Name() == DOC_LOOKUP_FILE {
			continue
		}
		// Open index file
		idxName := htFile.Name()
		idxPath := strings.Split(idxName, INDEX_PATH_SEP)
		col.indexPaths[idxName] = idxPath
		if col.hts[idxName], err = data.OpenHashTable(
			path.Join(col.db.path, col.name, idxName)); err != nil {
			return err
		}
	}
	return nil
}

// Close all collection files. Stop using the collection after the call.
func (col *Col) close() error {
	errs := make([]error, 0, 0)
	if err := col.part.Close(); err != nil {
		errs = append(errs, err)
	}
	for _, ht := range col.hts {
		if err := ht.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) == 0 {
		return nil
	}
	return fmt.Errorf("%v", errs)
}

// Run the function on each document, stop as soon as the function returns false.
func (col *Col) ForEachDoc(fun func(id uint64, doc []byte) (moveOn bool)) {
	// Process approx.4k documents in each iteration
	partDiv := col.ApproxDocCount() / 4000
	if partDiv == 0 {
		partDiv++
	}
	for i := uint64(0); i < partDiv; i++ {
		if !col.part.ForEachDoc(i, partDiv, fun) {
			return
		}
	}
}

// Return approximate number of documents in the collection.
func (col *Col) ApproxDocCount() uint64 {
	return col.part.ApproxDocCount()
}

// Partition documents into roughly equally sized portions, and run the function on each document in the portion.
func (col *Col) ForEachDocInPage(page, total uint64, fun func(id uint64, doc []byte) bool) {
	if !col.part.ForEachDoc(page, total, fun) {
		return
	}
}

// Return all indexed paths.
func (col *Col) AllIndexes() (ret [][]string) {
	ret = make([][]string, 0, len(col.indexPaths))
	for _, path := range col.indexPaths {
		pathCopy := make([]string, len(path))
		for i, p := range path {
			pathCopy[i] = p
		}
		ret = append(ret, pathCopy)
	}
	return ret
}

// Return all indexed paths. Index path segments are joint and return value is sorted.
func (col *Col) AllIndexesJointPaths() (ret []string) {
	ret = make([]string, 0, 0)
	for _, path := range col.AllIndexes() {
		ret = append(ret, strings.Join(path, INDEX_PATH_SEP))
	}
	sort.Strings(ret)
	return ret
}

// Remove an index.
func (col *Col) Unindex(idxPath []string) (err error) {
	idxName := strings.Join(idxPath, INDEX_PATH_SEP)
	if _, exists := col.indexPaths[idxName]; !exists {
		return fmt.Errorf("Path %v is not indexed", idxPath)
	}
	delete(col.indexPaths, idxName)
	col.hts[idxName].Close()
	delete(col.hts, idxName)
	err = os.RemoveAll(path.Join(col.db.path, col.name, idxName))
	return
}
