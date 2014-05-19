// Database logic - collection management.
package dbsvc

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/HouzuoGuo/tiedot/datasvc"
	"github.com/HouzuoGuo/tiedot/tdlog"
	"net/rpc"
	"os"
	"path"
	"strconv"
	"strings"
	"time"
)

// Construct a directory name for collection.
func (db *DBSvc) mkColDirName(colName string) string {
	return colName + COL_NAME_SPLIT + strconv.Itoa(db.totalRank)
}

// Get collection name and number of partitions from a collection directory name.
func (db *DBSvc) destructColDirName(dirName string) (string, int, error) {
	// Collection directory name looks like: "My_Wonderful_Stuff_8"
	split := strings.LastIndex(dirName, COL_NAME_SPLIT)
	if split == -1 {
		return "", 0, errors.New("Not a valid collection directory name")
	} else if split == 0 || split == len(dirName)-1 {
		return "", 0, errors.New("Not a valid collection directory name")
	} else if parts, err := strconv.Atoi(dirName[split+1:]); err != nil {
		return "", 0, errors.New("Not a valid collection directory name")
	} else {
		return dirName[0:split], parts, nil
	}
}

// Do fun for all documents in the collection.
func (db *DBSvc) forEachDoc(colName string, fun func(*rpc.Client, int, string) bool) error {
	totalIterations := 1000
	for iteratePart := 0; iteratePart < db.totalRank; iteratePart++ {
		tdlog.Printf("forEachDoc %s: Going through partition %d", colName, iteratePart)
		srv := db.data[iteratePart]
		for i := 0; i < totalIterations; i++ {
			var docs map[int]string
			if err := srv.Call("DataSvc.DocGetPartition", datasvc.DocGetPartitionInput{colName, i, totalIterations, db.mySchemaVersion}, &docs); err != nil {
				return err
			}
			for id, doc := range docs {
				if !fun(srv, id, doc) {
					return nil
				}
			}
		}
	}
	return nil
}

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
	if err := os.Rename(path.Join(db.dataDir, db.mkColDirName(oldName)), path.Join(db.dataDir, db.mkColDirName(newName))); err != nil {
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
	// Make a temporary collection
	tmpColName := fmt.Sprintf("scrub-%s-%d", name, time.Now().UnixNano())
	if err := os.MkdirAll(path.Join(db.dataDir, db.mkColDirName(tmpColName)), 0700); err != nil {
		return err
	}
	// Mirror indexes from original collection
	for _, idxPath := range db.schema[name] {
		if err := os.MkdirAll(path.Join(db.dataDir, db.mkColDirName(tmpColName), mkIndexDirName(idxPath)), 0700); err != nil {
			return err
		}
	}
	db.unloadAll()
	if err := db.loadSchema(true); err != nil {
		return err
	}
	// Temporary collection is now ready
	// Iterate through all documents in 1000 iterations, put them into the temporary collection
	db.forEachDoc(name, func(srv *rpc.Client, id int, doc string) bool {
		// Deserialize the document
		var docObj map[string]interface{}
		if err := json.Unmarshal([]byte(doc), &docObj); err != nil {
			return true
		}
		// The document goes to the partition it used to belong to
		if err := srv.Call("DataSvc.DocInsert", datasvc.DocInsertInput{tmpColName, strings.TrimSpace(doc), id, db.mySchemaVersion}, discard); err != nil {
			tdlog.Printf("Scrub %s: failed to insert document back, error - %v", name, err)
		}
		if err := db.indexDoc(tmpColName, id, docObj, false); err != nil {
			tdlog.Printf("Scrub %s: failed to index document, error - %v", name, err)
		}
		return true
	})
	db.unloadAll() // implicitly flushes all buffers
	// Replace the original collection by the temporary collection
	if err := os.RemoveAll(path.Join(db.dataDir, db.mkColDirName(name))); err != nil {
		return err
	}
	if err := os.Rename(path.Join(db.dataDir, db.mkColDirName(tmpColName)), path.Join(db.dataDir, db.mkColDirName(name))); err != nil {
		return err
	}
	// Reload all and done
	if err := db.loadSchema(true); err != nil {
		return err
	}
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
