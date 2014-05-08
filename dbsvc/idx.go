package dbsvc

import (
	"fmt"
	"os"
	"path"
)

// Create a new index.
func (db *DBSvc) IdxCreate(colName string, idxPath []string) error {
	db.lock.Lock()
	defer db.lock.Unlock()
	db.lockAllData()
	defer db.unlockAllData()
	idxUID := mkIndexUID(colName, idxPath)
	if err := db.loadSchema(false); err != nil {
		return err
	} else if _, exists := db.schema[colName]; !exists {
		return fmt.Errorf("Collection %s does not exist", colName)
	} else if _, exists := db.schema[colName][idxUID]; exists {
		return fmt.Errorf("Path %v is already indexed", idxPath)
	} else if err := os.MkdirAll(path.Join(db.dataDir, db.mkColDirName(colName), HT_DIR_MAGIC+idxUID), 0700); err != nil {
		return err
	}
	db.unloadAll()
	if err := db.loadSchema(true); err != nil {
		return err
	}
	return nil
}

// Return all indexed paths in a collection.
func (db *DBSvc) IdxAll(colName string) (paths [][]string, err error) {
	db.lock.Lock()
	defer db.lock.Unlock()
	if err = db.loadSchema(false); err != nil {
		return nil, err
	} else if _, exists := db.schema[colName]; !exists {
		return nil, fmt.Errorf("Collection %s does not exist", colName)
	}
	paths = make([][]string, 0, len(db.schema[colName]))
	for idxUID, _ := range db.schema[colName] {
		_, pathSegs := destructIndexUID(idxUID)
		paths = append(paths, pathSegs)
	}
	return
}

// Drop an index.
func (db *DBSvc) IdxDrop(colName string, idxPath []string) error {
	db.lock.Lock()
	defer db.lock.Unlock()
	db.lockAllData()
	defer db.unlockAllData()
	idxUID := mkIndexUID(colName, idxPath)
	if err := db.loadSchema(false); err != nil {
		return err
	} else if _, exists := db.schema[colName]; !exists {
		return fmt.Errorf("Collection %s does not exist", colName)
	} else if _, exists := db.schema[colName][idxUID]; !exists {
		return fmt.Errorf("Path %v is not indexed", idxPath)
	} else if err := os.RemoveAll(path.Join(db.dataDir, db.mkColDirName(colName), HT_DIR_MAGIC+idxUID)); err != nil {
		return err
	}
	return nil
}
