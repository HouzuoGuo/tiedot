package db

import (
	"github.com/HouzuoGuo/tiedot/data"
	"strconv"
	"path"
	"io/ioutil"
	"strings"
	"sync"
)


// Collection has data partitions and some index meta information.
type Col struct {
	parts []*data.Partition // document data partitions
	hts []map[string]*data.HashTable // index partitions
	indexPaths map[string][]string // index names and paths
	updating []map[int]struct{}
	partLock []*sync.RWMutex
}

// Database structures.
type DB struct {
	path string // root path of database directory
	numParts int // total number of partitions
	cols map[string]*Col // collection by name lookup
}

const (
	PART_NUM_FILE = "number_of_partitions"
	DOC_DATA_FILE = "dat_"
	DOC_LOOKUP_FILE = "id_"
	INDEX_PATH_SEP = "!"
)

// Open database and load all tables & indexes.
func OpenDB(dbPath string) (*DB, error) {
	db := &DB{dbPath}
	return db, db.load()
}

func (db *DB) Sync() error {
	errs := make([]error, 0, 0)
	for _, col := range db.cols {
		for i := 0; i < db.numParts; i++ {
			col.partLock[i].RLock()
			if err := col.parts[i].Sync(); err != nil {
				errs = append(errs, err)
			}
			col.partLock[i].RUnlock()
		}
	}
}

// Load all collection and index schema.
func (db *DB) load() error {
	// Get number of partitions from the text file
	if numParts, err := ioutil.ReadFile(path.Join(db.path, PART_NUM_FILE)); err != nil {
		return err
	} else if db.numParts, err = strconv.Atoi(string(numParts)); err != nil {
		return err
	}
	// Look for collection directories
	db.cols = make(map[string]*Col)
	dirContent, err := ioutil.ReadDir(db.path)
	if err != nil {
		return err
	}
	for _, maybeColDir := range dirContent {
		if !maybeColDir.IsDir() {
			continue
		}
		colName := maybeColDir.Name()
		theCol := &Col{
			parts: make([]*data.Partition, db.numParts), hts: make([]map[string]*data.HashTable, db.numParts), indexPaths: make(map[string][]string),
			updating: make([]map[int]struct{}, db.numParts), partLock: make([]*sync.RWMutex, db.numParts)}
		db.cols[colName] = theCol
		for i := 0; i < db.numParts; i++ {
			theCol.updating[i] = make(map[int]struct{})
			theCol.partLock[i] = new(sync.RWMutex)
		}
		// Open document data partitions
		for i := 0; i < db.numParts; i++ {
			if theCol.parts[i], err = data.OpenPartition(path.Join(db.path, colName, DOC_DATA_FILE + strconv.Itoa(i)), path.Join(db.path, colName, DOC_LOOKUP_FILE + strconv.Itoa(i))); err != nil {
				return err
			}
		}
		// Look for index directories
		colDirContent, err := ioutil.ReadDir(path.Join(db.path, colName))
		if err != nil {
			return err
		}
		for _, htDir := range colDirContent {
			if (!htDir.IsDir()) {
				continue
			}
			idxName := htDir.Name()
			idxPath := strings.Split(idxName, INDEX_PATH_SEP)
			theCol.indexPaths[idxName] = idxPath
			for i := 0; i < db.numParts; i++ {
				theCol.hts[i] = make(map[string]*data.HashTable)
				if theCol.hts[i][idxName], err = data.OpenHashTable(path.Join(db.path, colName, idxName, strconv.Itoa(i))); err != nil {
					return err
				}
			}
		}
	}
	return err
}
