// DB sharding via IPC using a binary protocol - collection/index creation and maintenance.
package sharding

import (
	"fmt"
	"github.com/HouzuoGuo/tiedot/data"
	"github.com/HouzuoGuo/tiedot/tdlog"
	"path"
	"sort"
	"strconv"
	"time"
)

// Create a new collection.
func (client *RouterClient) Create(colName string) error {
	return client.reqMaintAccess(func() error {
		dbfs, err := data.DBReadDir(client.dbdir)
		if err != nil {
			return err
		}
		return dbfs.CreateCollection(colName)
	})
}

// Return all collection names, sorted in alphabetical order.
func (client *RouterClient) AllCols() (names []string) {
	if err := client.Ping(); err != nil {
		tdlog.Noticef("Client %d: failed to ping before returning collection names - %v", client.id, err)
	}
	client.opLock.Lock()
	names = client.dbo.GetAllColNames()
	client.opLock.Unlock()
	return
}

// Rename a collection.
func (client *RouterClient) Rename(oldName, newName string) error {
	return client.reqMaintAccess(func() error {
		dbfs, err := data.DBReadDir(client.dbdir)
		if err != nil {
			return err
		}
		return dbfs.RenameCollection(oldName, newName)
	})
}

// Truncate a collection - fast delete all documents and clear all indexes.
func (client *RouterClient) Truncate(colName string) error {
	return client.reqMaintAccess(func() error {
		dbfs, err := data.DBReadDir(client.dbdir)
		if err != nil {
			return err
		}
		return dbfs.Truncate(colName)
	})
}

// De-fragment collection free-space and get rid of corrupted documents.
func (client *RouterClient) Scrub(colName string) error {
	return client.reqMaintAccess(func() error {
		// Remember existing indexes
		existingIndexes := make([][]string, 0, 0)
		colID, exists := client.schema.colNameLookup[colName]
		if !exists {
			return fmt.Errorf("Collection does not exist")
		}
		for _, existingIndex := range client.schema.indexPaths[colID] {
			existingIndexes = append(existingIndexes, existingIndex)
		}
		// Create a temporary collection for holding good&clean documents
		tmpColName := fmt.Sprintf("scrub-%s-%d", colName, time.Now().UnixNano())
		err := client.forAllDBsDo(func(clientDB *db.DB) error {
			if err := clientDB.Create(tmpColName); err != nil {
				return err
			}
			// Recreate all indexes
			for _, existingIndex := range existingIndexes {
				if err := clientDB.Use(tmpColName).BPIndex(existingIndex); err != nil {
					return err
				}
			}
			return nil
		})
		// Reload schema so that servers & client know the temp collection
		if err != nil {
			return err
		} else if err = client.reloadServer(); err != nil {
			return err
		}
		tmpColID, exists := client.schema.colNameLookup[tmpColName]
		if !exists {
			return fmt.Errorf("TmpCol went missing?!")
		}
		// Put documents back in - 10k at a time
		docCount, err := client.approxDocCount(colName)
		if err != nil {
			return err
		}
		total := docCount/10000 + 1
		for page := uint64(0); page < total; page++ {
			docs, err := client.getDocPage(colName, page, total, true)
			if err != nil {
				return err
			}
			for docID, doc := range docs {
				if err := client.insertRecovery(tmpColID, docID, doc); err != nil {
					return err
				}
			}
		}
		// Replace the original collection by the good&clean one
		dbfs, err := data.DBReadDir(client.dbdir)
		if err != nil {
			return err
		}
		err = dbfs.DropCollection(colName)
		if err != nil {
			return err
		}
		err = dbfs.RenameCollection(tmpColName, colName)
		if err != nil {
			return err
		}
		return client.reloadServer()
	})
}

// Drop a collection.
func (client *RouterClient) Drop(colName string) error {
	return client.reqMaintAccess(func() error {
		dbfs, err := data.DBReadDir(client.dbdir)
		if err != nil {
			return err
		}
		return dbfs.DropCollection(colName)
	})
}

// Copy database into destination directory (for backup).
func (client *RouterClient) DumpDB(destDir string) error {
	return client.reqMaintAccess(func() error {
		dbfs, err := data.DBReadDir(client.dbdir)
		if err != nil {
			return err
		}
		return dbfs.Backup(destDir)
	})
}

// Create an index.
func (client *RouterClient) Index(colName string, idxPath []string) error {
	return client.reqMaintAccess(func() error {
		for i := 0; i < client.nProcs; i++ {
			if clientDB, err := db.OpenDB(path.Join(client.workspace, strconv.Itoa(i))); err != nil {
				return err
			} else if clientDB.Use(colName) == nil {
				return fmt.Errorf("Collection does not exist")
			} else if err = clientDB.Use(colName).BPIndex(idxPath); err != nil {
				return err
			} else if err = clientDB.Close(); err != nil {
				return err
			}
		}
		// Refresh schema on server and myself
		if err := client.reloadServer(); err != nil {
			return err
		}
		// Figure out the hash table ID
		newHTID := client.schema.GetHTIDByPath(colName, idxPath)
		if newHTID == -1 {
			return fmt.Errorf("New hash table is missing?!")
		}
		htIDBytes := Bint32(newHTID)
		// Reindex documents - 10k at a time
		docCount, err := client.approxDocCount(colName)
		if err != nil {
			return err
		}
		total := docCount/10000 + 1
		for page := uint64(0); page < total; page++ {
			docs, err := client.getDocPage(colName, page, total, true)
			if err != nil {
				return err
			}
			// A simplified client.indexDoc
			for docID, doc := range docs {
				docIDBytes := Buint64(docID)
				for _, val := range ResolveDocAttr(doc, idxPath) {
					if val != nil {
						htKey := StringHash(fmt.Sprint(val))
						if _, _, err := client.sendCmd(int(htKey%uint64(client.nProcs)), false, C_HT_PUT, htIDBytes, Buint64(htKey), docIDBytes); err != nil {
							return err
						}
					}
				}
			}
		}
		return nil
	})
}

// Return all indexed paths in a collection, sorted in alphabetical order.
func (client *RouterClient) AllIndexes(colName string) (paths [][]string, err error) {
	jointPath, err := client.AllIndexesJointPaths(colName)
	if err != nil {
		return
	}
	paths = make([][]string, len(jointPath))
	for i, aPath := range jointPath {
		paths[i] = data.SplitIndexPath(aPath)
	}
	return
}

// Return all indexed paths. Index path segments are joint together, and results are sorted in alphabetical order.
func (client *RouterClient) AllIndexesJointPaths(colName string) (paths []string, err error) {
	paths = make([]string, 0, 0)
	if err := client.Ping(); err != nil {
		tdlog.Noticef("Client %d: failed to ping before returning index paths - %v", client.id, err)
	}
	client.opLock.Lock()
	paths, err = client.dbo.GetAllIndexPaths(colName)
	client.opLock.Unlock()
	sort.Strings(paths)
	return
}

// Remove an index.
func (client *RouterClient) Unindex(colName string, idxPath []string) error {
	return client.reqMaintAccess(func() error {
		dbfs, err := data.DBReadDir(client.dbdir)
		if err != nil {
			return err
		}
		return dbfs.DropIndex(colName, data.JoinIndexPath(idxPath))
	})
}
