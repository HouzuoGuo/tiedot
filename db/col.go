/* Collection schema and index management. */
package db

import (
	"encoding/json"
	"fmt"
	"github.com/HouzuoGuo/tiedot/data"
	"github.com/HouzuoGuo/tiedot/tdlog"
	"io/ioutil"
	"os"
	"path"
	"strconv"
	"strings"
)

const (
	DOC_DATA_FILE   = "dat_"
	DOC_LOOKUP_FILE = "id_"
	INDEX_PATH_SEP  = "!"
)

// Collection has data partitions and some index meta information.
type Col struct {
	db         *DB
	name       string
	parts      []*data.Partition            // document data partitions
	hts        []map[string]*data.HashTable // index partitions
	indexPaths map[string][]string          // index names and paths
}

// Open a collection and load all indexes.
func OpenCol(db *DB, name string) (*Col, error) {
	col := &Col{db: db, name: name}
	return col, col.load()
}

// Load collection schema including index schema.
func (col *Col) load() error {
	col.parts = make([]*data.Partition, col.db.numParts)
	col.hts = make([]map[string]*data.HashTable, col.db.numParts)
	for i := 0; i < col.db.numParts; i++ {
		col.hts[i] = make(map[string]*data.HashTable)
	}
	col.indexPaths = make(map[string][]string)
	// Open document data partitions
	for i := 0; i < col.db.numParts; i++ {
		var err error
		if col.parts[i], err = data.OpenPartition(
			path.Join(col.db.path, col.name, DOC_DATA_FILE+strconv.Itoa(i)),
			path.Join(col.db.path, col.name, DOC_LOOKUP_FILE+strconv.Itoa(i))); err != nil {
			return err
		}
	}
	// Look for index directories
	colDirContent, err := ioutil.ReadDir(path.Join(col.db.path, col.name))
	if err != nil {
		return err
	}
	for _, htDir := range colDirContent {
		if !htDir.IsDir() {
			continue
		}
		idxName := htDir.Name()
		idxPath := strings.Split(idxName, INDEX_PATH_SEP)
		col.indexPaths[idxName] = idxPath
		for i := 0; i < col.db.numParts; i++ {
			col.hts[i] = make(map[string]*data.HashTable)
			if col.hts[i][idxName], err = data.OpenHashTable(
				path.Join(col.db.path, col.name, idxName, strconv.Itoa(i))); err != nil {
				return err
			}
		}
	}
	return nil
}

// Synchronize all data files to disk.
func (col *Col) Sync() error {
	errs := make([]error, 0, 0)
	for i := 0; i < col.db.numParts; i++ {
		col.parts[i].Lock.Lock()
		if err := col.parts[i].Sync(); err != nil {
			errs = append(errs, err)
		}
		for _, ht := range col.hts[i] {
			if err := ht.Sync(); err != nil {
				errs = append(errs, err)
			}
		}
		col.parts[i].Lock.Unlock()
	}
	if len(errs) == 0 {
		return nil
	}
	return fmt.Errorf("%v", errs)
}

// Do fun for all documents in the collection.
func (col *Col) ForEachDoc(withRLocks bool, fun func(id int, doc []byte) (moveOn bool)) {
	totalIterations := 1993 // not a magic - feel free to adjust the number
	for iteratePart := 0; iteratePart < col.db.numParts; iteratePart++ {
		tdlog.Printf("ForEachDoc %s: Going through partition %d", col.name, iteratePart)
		part := col.parts[iteratePart]
		part.Lock.RLock()
		for i := 0; i < totalIterations; i++ {
			if !part.ForEachDoc(i, totalIterations, fun) {
				tdlog.Printf("ForEachDoc %s: Stopped on collection partition %d, hash partition %d", iteratePart, i)
				part.Lock.RUnlock()
				return
			}
		}
		part.Lock.RUnlock()
	}
}

// Close all collection files. Do not use the collection afterwards!
func (col *Col) Close() error {
	errs := make([]error, 0, 0)
	for i := 0; i < col.db.numParts; i++ {
		col.parts[i].Lock.Lock()
		if err := col.parts[i].Close(); err != nil {
			errs = append(errs, err)
		}
		for _, ht := range col.hts[i] {
			if err := ht.Close(); err != nil {
				errs = append(errs, err)
			}
		}
		col.parts[i].Lock.Unlock()
	}
	if len(errs) == 0 {
		return nil
	}
	return fmt.Errorf("%v", errs)
}

// Create an index on the path.
func (col *Col) Index(idxPath []string) (err error) {
	idxName := strings.Join(idxPath, INDEX_PATH_SEP)
	if _, exists := col.indexPaths[idxName]; exists {
		return fmt.Errorf("Path %v is already indexed", idxPath)
	}
	col.indexPaths[idxName] = idxPath
	idxDir := path.Join(col.db.path, col.name, idxName)
	if err = os.MkdirAll(idxDir, 0700); err != nil {
		return err
	}
	for i := 0; i < col.db.numParts; i++ {
		if col.hts[i][idxName], err = data.OpenHashTable(path.Join(idxDir, strconv.Itoa(i))); err != nil {
			return err
		}
	}
	// Put all documents on the new index
	col.ForEachDoc(false, func(id int, doc []byte) (moveOn bool) {
		var docObj map[string]interface{}
		if err := json.Unmarshal(doc, &docObj); err != nil {
			// Skip corrupted document
			return true
		}
		for _, idxVal := range GetIn(docObj, idxPath) {
			if idxVal != nil {
				hashKey := StrHash(fmt.Sprint(idxVal))
				col.hts[hashKey%col.db.numParts][idxName].Put(hashKey, id)
			}
		}
		return true
	})
	return
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

// Remove an index.
func (col *Col) Unindex(idxPath []string) error {
	idxName := strings.Join(idxPath, INDEX_PATH_SEP)
	if _, exists := col.indexPaths[idxName]; !exists {
		return fmt.Errorf("Path %v is not indexed", idxPath)
	}
	delete(col.indexPaths, idxName)
	for i := 0; i < col.db.numParts; i++ {
		col.hts[i][idxName].Close()
		delete(col.hts[i], idxName)
	}
	if err := os.RemoveAll(path.Join(col.db.path, col.name, idxName)); err != nil {
		return err
	}
	return nil
}
