// Database logic - collection management.
package dbsvc

import (
	"errors"
	"fmt"
	"os"
	"path"
)

// Create a new collection.
func (db *DBSvc) ColCreate(name string) error {
	db.lock.Lock()
	defer db.lock.Unlock()
	db.lockAllData()
	defer db.unlockAllData()
	if err := db.loadSchema(false); err != nil {
		return err
	}
	if _, exists := db.schema[name]; exists {
		return fmt.Errorf("Collection %s already exists", name)
	}
	// Make directory for new collection and reload everything
	colDirName := db.mkColDirName(name)
	if err := os.MkdirAll(path.Join(db.dataDir, colDirName), 0700); err != nil {
		return err
	}
	db.unloadAll()
	if err := db.loadSchema(true); err != nil {
		return err
	}
	return nil
}

// Return all collection names.
func (db *DBSvc) ColAll() (names []string, err error) {
	db.lock.Lock()
	defer db.lock.Unlock()
	if err = db.loadSchema(false); err != nil {
		return
	}
	names = make([]string, 0, len(db.schema))
	for name, _ := range db.schema {
		names = append(names, name)
	}
	return
}

// Rename a collection.
func (db *DBSvc) ColRename(oldName string, newName string) error {
	if oldName == newName {
		return errors.New("Old and new collection names are the same")
	}
	db.lock.Lock()
	defer db.lock.Unlock()
	db.lockAllData()
	defer db.unlockAllData()
	if err := db.loadSchema(false); err != nil {
		return err
	} else if _, exists := db.schema[oldName]; !exists {
		return fmt.Errorf("Collection %s does not exist", oldName)
	} else if _, exists := db.schema[newName]; exists {
		return fmt.Errorf("Collection %s already exists", newName)
	}
	db.unloadAll()
	oldDirName := db.mkColDirName(oldName)
	newDirName := db.mkColDirName(newName)
	if err := os.Rename(path.Join(db.dataDir, oldDirName), path.Join(db.dataDir, newDirName)); err != nil {
		return err
	} else if err := db.loadSchema(true); err != nil {
		return err
	}
	return nil
}

// Truncate a collection - delete all document data, and clear all indexes.
func (db *DBSvc) ColTruncate(name string) error {
	db.lock.Lock()
	defer db.lock.Unlock()
	db.lockAllData()
	defer db.unlockAllData()
	if err := db.loadSchema(false); err != nil {
		return err
	} else if _, exists := db.schema[name]; !exists {
		return fmt.Errorf("Collection %s does not exist", name)
	}
	for _, srv := range db.data {
		if err := srv.Call("DataSvc.PartClear", name, discard); err != nil {
			return err
		}
		for idxName, _ := range db.schema[name] {
			if err := srv.Call("DataSvc.HTClear", idxName, discard); err != nil {
				return err
			}
		}
	}
	return nil
}

// Repair and compress a collection.
func (db *DBSvc) ColScrub(name string) error {
	db.lock.Lock()
	defer db.lock.Unlock()
	db.lockAllData()
	defer db.unlockAllData()
	if err := db.loadSchema(false); err != nil {
		return err
	} else if _, exists := db.schema[name]; !exists {
		return fmt.Errorf("Collection %s does not exist", name)
	}
	// TODO: re-create the collection and its documents
	return nil
}

// Drop a collection.
func (db *DBSvc) ColDrop(name string) error {
	db.lock.Lock()
	defer db.lock.Unlock()
	db.lockAllData()
	defer db.unlockAllData()
	if err := db.loadSchema(false); err != nil {
		return err
	} else if _, exists := db.schema[name]; !exists {
		return fmt.Errorf("Collection %s does not exist", name)
	}
	db.unloadAll()
	dirName := db.mkColDirName(name)
	if err := os.RemoveAll(path.Join(db.dataDir, dirName)); err != nil {
		return err
	} else if err := db.loadSchema(true); err != nil {
		return err
	}
	return nil
}
