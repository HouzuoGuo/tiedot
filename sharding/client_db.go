// DB sharding via IPC using a binary protocol - collection/index creation and maintenance.
package sharding

import (
	"fmt"
	"github.com/HouzuoGuo/tiedot/data"
	"github.com/HouzuoGuo/tiedot/tdlog"
	"sort"
	"time"
)

// Create a new collection.
func (client *RouterClient) Create(colName string) error {
	return client.reqMaintAccess(func() error {
		return client.dbo.GetDBFS().CreateCollection(colName)
	})
}

// Return all collection names, sorted in alphabetical order.
func (client *RouterClient) AllCols() (names []string) {
	if err := client.Ping(); err != nil {
		tdlog.Noticef("Client %d: failed to ping before returning collection names - %v", client.id, err)
	}
	client.opLock.Lock()
	names = client.dbo.GetDBFS().GetCollectionNamesSorted()
	client.opLock.Unlock()
	return
}

// Rename a collection.
func (client *RouterClient) Rename(oldName, newName string) error {
	return client.reqMaintAccess(func() error {
		return client.dbo.GetDBFS().RenameCollection(oldName, newName)
	})
}

// Truncate a collection - fast delete all documents and clear all indexes.
func (client *RouterClient) Truncate(colName string) error {
	return client.reqMaintAccess(func() error {
		return client.dbo.GetDBFS().Truncate(colName)
	})
}

// De-fragment collection free-space and get rid of corrupted documents.
func (client *RouterClient) Scrub(colName string) error {
	return client.reqMaintAccess(func() (err error) {
		found := false
		for _, name := range client.dbo.GetDBFS().GetCollectionNamesSorted() {
			if name == colName {
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("Collection %s does not exist", colName)
		}
		// Remember existing indexes
		existingIndexes, _ := client.dbo.GetDBFS().GetIndexesSorted(colName)
		// Create a temporary collection for holding good&clean documents
		tmpColName := fmt.Sprintf("scrub-%s-%d", colName, time.Now().UnixNano())
		if err = client.dbo.GetDBFS().CreateCollection(tmpColName); err != nil {
			return
		}
		// Recreate all indexes
		for _, existingIndex := range existingIndexes {
			if err = client.dbo.GetDBFS().CreateIndex(tmpColName, existingIndex); err != nil {
				return
			}
		}
		// Reload both server and to make the temp collection known and loaded
		if err = client.reloadServer(); err != nil {
			return
		}
		tmpColID, exists := client.dbo.GetColIDByName(tmpColName)
		if !exists {
			return fmt.Errorf("TmpCol went missing?!")
		}
		// Put documents back in - 10k at a time
		docCount, err := client.approxDocCount(colName)
		if err != nil {
			return
		}
		total := docCount/10000 + 1
		for page := uint64(0); page < total; page++ {
			docs, err := client.getDocPage(colName, page, total, true)
			if err != nil {
				return err
			}
			for docID, doc := range docs {
				if err = client.insertRecovery(tmpColID, docID, doc); err != nil {
					return err
				}
			}
		}
		// Replace the original collection by the good&clean one
		if err = client.dbo.GetDBFS().DropCollection(colName); err != nil {
			return
		} else if err = client.dbo.GetDBFS().RenameCollection(tmpColName, colName); err != nil {
			return
		}
		return client.reloadServer()
	})
}

// Drop a collection.
func (client *RouterClient) Drop(colName string) error {
	return client.reqMaintAccess(func() error {
		return client.dbo.GetDBFS().DropCollection(colName)
	})
}

// Copy database into destination directory (for backup).
func (client *RouterClient) Backup(destDir string) error {
	return client.reqMaintAccess(func() error {
		return client.dbo.GetDBFS().Backup(destDir)
	})
}

// Create an index.
func (client *RouterClient) Index(colName string, idxPath []string) error {
	return client.reqMaintAccess(func() error {
		// Create the new index
		if err := client.dbo.GetDBFS().CreateIndex(colName, data.JoinIndexPath(idxPath)); err != nil {
			return err
		}
		// Refresh schema on server and myself
		if err := client.reloadServer(); err != nil {
			return err
		}
		// Figure out the hash table ID
		colID, exists := client.dbo.GetColIDByName(colName)
		if !exists {
			return fmt.Errorf("New collection went missing?!")
		}
		newHTID := client.dbo.GetIndexIDBySplitPath(colID, idxPath)
		if newHTID == -1 {
			return fmt.Errorf("New hash table went missing?!")
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
						//						fmt.Printf("Reindex will put doc %d value %v on key %d\n", docID, val, htKey)
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
	paths, err = client.dbo.GetDBFS().GetIndexesSorted(colName)
	client.opLock.Unlock()
	sort.Strings(paths)
	return
}

// Remove an index.
func (client *RouterClient) Unindex(colName string, idxPath []string) error {
	return client.reqMaintAccess(func() error {
		return client.dbo.GetDBFS().DropIndex(colName, data.JoinIndexPath(idxPath))
	})
}
