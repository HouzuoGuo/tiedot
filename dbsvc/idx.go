package dbsvc

import (
	"encoding/json"
	"fmt"
	"github.com/HouzuoGuo/tiedot/datasvc"
	"github.com/HouzuoGuo/tiedot/tdlog"
	"net/rpc"
	"os"
	"path"
	"strings"
)

// Make a unique ID name for identifying an indexed path in a document collection.
func mkIndexUID(colName string, idxPath []string) string {
	together := make([]string, len(idxPath)+1)
	together[0] = colName
	copy(together[1:], idxPath)
	return strings.Join(together, IDX_PATH_SPLIT)
}

// Get collection name and indexed path from an index ID.
func destructIndexUID(indexUID string) (colName string, idxPath []string) {
	splitted := strings.Split(indexUID, IDX_PATH_SPLIT)
	return splitted[0], splitted[1:]
}

// Make a directory name for an indexed path.
func mkIndexDirName(idxPath []string) string {
	return HT_DIR_MAGIC + strings.Join(idxPath, IDX_PATH_SPLIT)
}

// Create a new index.
func (db *DBSvc) IdxCreate(colName string, idxPath []string) error {
	db.lock.Lock()
	defer db.lock.Unlock()
	db.lockAllData()
	defer db.unlockAllData()
	idxName := mkIndexUID(colName, idxPath)
	if err := db.loadSchema(false); err != nil {
		return err
	} else if _, exists := db.schema[colName]; !exists {
		return fmt.Errorf("Collection %s does not exist", colName)
	} else if _, exists := db.schema[colName][idxName]; exists {
		return fmt.Errorf("Path %v is already indexed", idxPath)
	} else if err := os.MkdirAll(path.Join(db.dataDir, db.mkColDirName(colName), mkIndexDirName(idxPath)), 0700); err != nil {
		return err
	}
	db.unloadAll()
	if err := db.loadSchema(true); err != nil {
		return err
	}
	// Index all documents on the new index
	db.forEachDoc(colName, func(srv *rpc.Client, id int, doc string) bool {
		var docObj map[string]interface{}
		if err := json.Unmarshal([]byte(doc), &docObj); err != nil {
			return true
		}
		for _, idxVal := range GetIn(docObj, idxPath) {
			hashKey := StrHash(fmt.Sprint(idxVal))
			hashPart := db.data[hashKey%db.totalRank]
			if err := hashPart.Call("DataSvc.HTPut", datasvc.HTPutInput{idxName, hashKey, id, db.mySchemaVersion}, discard); err != nil {
				tdlog.Printf("IdxCreate %s: failed to index document on the new index, error - %v", idxName, err)
				return true
			}
		}
		return true
	})
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
	if err := db.loadSchema(false); err != nil {
		return err
	} else if _, exists := db.schema[colName]; !exists {
		return fmt.Errorf("Collection %s does not exist", colName)
	} else if _, exists := db.schema[colName][mkIndexUID(colName, idxPath)]; !exists {
		return fmt.Errorf("Path %v is not indexed", idxPath)
	} else if err := os.RemoveAll(path.Join(db.dataDir, db.mkColDirName(colName), mkIndexDirName(idxPath))); err != nil {
		return err
	}
	return nil
}
