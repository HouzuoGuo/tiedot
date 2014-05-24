package db

import (
	"fmt"
	"github.com/HouzuoGuo/tiedot/data"
	"io/ioutil"
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

// Close all collection files.
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
	col.indexPaths = make(map[string][]string)
	col.parts = make([]*data.Partition, 0)
	col.hts = make([]map[string]*data.HashTable, 0)
	if len(errs) == 0 {
		return nil
	}
	return fmt.Errorf("%v", errs)
}
