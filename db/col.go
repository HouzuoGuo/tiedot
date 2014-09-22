/* Collection schema and index management. */
package db

import (
	"encoding/json"
	"fmt"
	"github.com/HouzuoGuo/tiedot/data"
	"io/ioutil"
	"os"
	"path"
	"sort"
	"strings"
)

const (
	DOC_DATA_FILE   = "dat" // Prefix of partition collection data file name.
	DOC_LOOKUP_FILE = "id"  // Prefix of partition hash table (ID lookup) file name.
	INDEX_PATH_SEP  = "!"   // Separator between index keys in index directory name.
)

// Collection has data partitions and some index meta information.
type Col struct {
	db         *DB
	name       string
	part       *data.Partition            // Collection partitions
	hts        map[string]*data.HashTable // Index partitions
	indexPaths map[string][]string        // Index names and paths
	locked     map[uint64]bool            // (BinProt only) documents that are locked for update
}

// Open a collection and load all indexes.
func OpenCol(db *DB, name string) (*Col, error) {
	col := &Col{db: db, name: name, locked: make(map[uint64]bool)}
	return col, col.load()
}

// Load collection schema including index schema.
func (col *Col) load() error {
	if err := os.MkdirAll(path.Join(col.db.path, col.name), 0700); err != nil {
		return err
	}
	col.hts = make(map[string]*data.HashTable)
	col.indexPaths = make(map[string][]string)
	// Open collection document partition
	var err error
	if col.part, err = data.OpenPartition(
		path.Join(col.db.path, col.name, DOC_DATA_FILE),
		path.Join(col.db.path, col.name, DOC_LOOKUP_FILE)); err != nil {
		return err
	}
	// Look for index directories
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

// Close collection files. Do not use the collection afterwards!
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

// Do fun for all documents in the collection.
func (col *Col) forEachDoc(fun func(id uint64, doc []byte) (moveOn bool)) {
	// Process approx.4k documents in each iteration
	partDiv := col.approxDocCount()
	if partDiv == 0 {
		partDiv++
	}
	for i := uint64(0); i < partDiv; i++ {
		if !col.part.ForEachDoc(i, partDiv, fun) {
			return
		}
	}
}

func (col *Col) BPUseHT(jointPath string) *data.HashTable {
	col.db.lock.RLock()
	ret := col.hts[jointPath]
	col.db.lock.RUnlock()
	return ret
}

// Do fun for all documents in the collection.
func (col *Col) ForEachDoc(fun func(id uint64, doc []byte) (moveOn bool)) {
	col.db.lock.RLock()
	col.forEachDoc(fun)
	col.db.lock.RUnlock()
}

// Create an index on the path.
func (col *Col) Index(idxPath []string) (err error) {
	idxName := strings.Join(idxPath, INDEX_PATH_SEP)
	col.db.lock.Lock()
	if _, exists := col.indexPaths[idxName]; exists {
		col.db.lock.Unlock()
		return fmt.Errorf("Path %v is already indexed", idxPath)
	}
	col.indexPaths[idxName] = idxPath
	idxFileName := path.Join(col.db.path, col.name, idxName)
	if col.hts[idxName], err = data.OpenHashTable(idxFileName); err != nil {
		col.db.lock.Unlock()
		return err
	}
	// Put all documents on the new index
	col.forEachDoc(func(id uint64, doc []byte) (moveOn bool) {
		var docObj map[string]interface{}
		if err := json.Unmarshal(doc, &docObj); err != nil {
			// Skip corrupted document
			return true
		}
		for _, idxVal := range GetIn(docObj, idxPath) {
			if idxVal != nil {
				hashKey := StrHash(fmt.Sprint(idxVal))
				col.hts[idxName].Put(hashKey, id)
			}
		}
		return true
	})
	col.db.lock.Unlock()
	return
}

// Return all indexed paths.
func (col *Col) AllIndexes() (ret [][]string) {
	col.db.lock.RLock()
	ret = make([][]string, 0, len(col.indexPaths))
	for _, path := range col.indexPaths {
		pathCopy := make([]string, len(path))
		for i, p := range path {
			pathCopy[i] = p
		}
		ret = append(ret, pathCopy)
	}
	col.db.lock.RUnlock()
	return ret
}

// Return all indexed paths. Index path segments are joint.
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
	col.db.lock.Lock()
	if _, exists := col.indexPaths[idxName]; !exists {
		col.db.lock.Unlock()
		return fmt.Errorf("Path %v is not indexed", idxPath)
	}
	delete(col.indexPaths, idxName)
	col.hts[idxName].Close()
	delete(col.hts, idxName)
	err = os.RemoveAll(path.Join(col.db.path, col.name, idxName))
	col.db.lock.Unlock()
	return
}

func (col *Col) approxDocCount() uint64 {
	return col.part.ApproxDocCount()
}

// Return approximate number of documents in the collection.
func (col *Col) ApproxDocCount() (ret uint64) {
	col.db.lock.RLock()
	ret = col.part.ApproxDocCount()
	col.db.lock.RUnlock()
	return
}

// Divide the collection into roughly equally sized pages, and do fun on all documents in the specified page.
func (col *Col) ForEachDocInPage(page, total uint64, fun func(id uint64, doc []byte) bool) {
	col.db.lock.RLock()
	if !col.part.ForEachDoc(page, total, fun) {
		col.db.lock.RUnlock()
		return
	}
	col.db.lock.RUnlock()
}
