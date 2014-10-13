// Binary protocol over IPC - DB management features (client).

package binprot

import (
	"fmt"
	"github.com/HouzuoGuo/tiedot/db"
	"github.com/HouzuoGuo/tiedot/tdlog"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
)

// Create a new collection.
func (client *BinProtClient) Create(colName string) error {
	return client.reqMaintAccess(func() error {
		for i := 0; i < client.nProcs; i++ {
			if clientDB, err := db.OpenDB(path.Join(client.workspace, strconv.Itoa(i))); err != nil {
				return err
			} else if err = clientDB.Create(colName); err != nil {
				return err
			} else if err = clientDB.Close(); err != nil {
				return err
			}
		}
		return nil
	})
}

// Return all collection names sorted in alphabetical order.
func (client *BinProtClient) AllCols() (names []string) {
	if err := client.Ping(); err != nil {
		tdlog.Noticef("Client %d: failed to ping before returning collection names - %v", client.id, err)
	}
	client.opLock.Lock()
	names = make([]string, 0, len(client.colNameLookup))
	for name := range client.colNameLookup {
		names = append(names, name)
	}
	client.opLock.Unlock()
	sort.Strings(names)
	return
}

// Rename a collection.
func (client *BinProtClient) Rename(oldName, newName string) error {
	return client.reqMaintAccess(func() error {
		for i := 0; i < client.nProcs; i++ {
			if clientDB, err := db.OpenDB(path.Join(client.workspace, strconv.Itoa(i))); err != nil {
				return err
			} else if err = clientDB.Rename(oldName, newName); err != nil {
				return err
			} else if err = clientDB.Close(); err != nil {
				return err
			}
		}
		return nil
	})
}

// Truncate a collection
func (client *BinProtClient) Truncate(colName string) error {
	return client.reqMaintAccess(func() error {
		for i := 0; i < client.nProcs; i++ {
			if clientDB, err := db.OpenDB(path.Join(client.workspace, strconv.Itoa(i))); err != nil {
				return err
			} else if err = clientDB.Truncate(colName); err != nil {
				return err
			} else if err = clientDB.Close(); err != nil {
				return err
			}
		}
		return nil
	})
}

// Compact a collection and recover corrupted documents.
func (client *BinProtClient) Scrub(colName string) error {
	return client.reqMaintAccess(func() error {
		return nil
	})
}

// Drop a collection.
func (client *BinProtClient) Drop(colName string) error {
	return client.reqMaintAccess(func() error {
		for i := 0; i < client.nProcs; i++ {
			if clientDB, err := db.OpenDB(path.Join(client.workspace, strconv.Itoa(i))); err != nil {
				return err
			} else if err = clientDB.Drop(colName); err != nil {
				return err
			} else if err = clientDB.Close(); err != nil {
				return err
			}
		}
		return nil
	})
}

// Copy database into destination directory (for backup).
func (client *BinProtClient) DumpDB(destDir string) error {
	return client.reqMaintAccess(func() error {
		for i := 0; i < client.nProcs; i++ {
			destDirPerRank := path.Join(destDir, strconv.Itoa(i))
			if err := os.MkdirAll(destDirPerRank, 0700); err != nil {
				return err
			} else if clientDB, err := db.OpenDB(path.Join(client.workspace, strconv.Itoa(i))); err != nil {
				return err
			} else if err = clientDB.Dump(destDirPerRank); err != nil {
				return err
			} else if err = clientDB.Close(); err != nil {
				return err
			}
		}
		return nil
	})
}

// Create an index.
func (client *BinProtClient) Index(colName string, idxPath []string) error {
	return client.reqMaintAccess(func() error {
		for i := 0; i < client.nProcs; i++ {
			if clientDB, err := db.OpenDB(path.Join(client.workspace, strconv.Itoa(i))); err != nil {
				return err
			} else if clientDB.Use(colName) == nil {
				return fmt.Errorf("Collection does not exist")
			} else if err = clientDB.Use(colName).Index(idxPath); err != nil {
				return err
			} else if err = clientDB.Close(); err != nil {
				return err
			}
		}
		return nil
	})
}

// Return all indexed paths in a collection. Results are sorted in alphabetical order.
func (client *BinProtClient) AllIndexes(colName string) (paths [][]string, err error) {
	jointPath, err := client.AllIndexesJointPaths(colName)
	if err != nil {
		return
	}
	paths = make([][]string, len(jointPath))
	for i, aPath := range jointPath {
		paths[i] = strings.Split(aPath, db.INDEX_PATH_SEP)
	}
	return
}

// Return all indexed paths. Index path segments are joint together, and results are sorted in alphabetical order.
func (client *BinProtClient) AllIndexesJointPaths(colName string) (paths []string, err error) {
	paths = make([]string, 0, 0)
	if err := client.Ping(); err != nil {
		tdlog.Noticef("Client %d: failed to ping before returning index paths - %v", client.id, err)
	}
	client.opLock.Lock()
	colID, exists := client.colNameLookup[colName]
	if !exists {
		client.opLock.Unlock()
		return nil, fmt.Errorf("Collection %s does not exist", colName)
	}
	// Join and sort
	for _, pathSegs := range client.indexPaths[colID] {
		paths = append(paths, strings.Join(pathSegs, db.INDEX_PATH_SEP))
	}
	sort.Strings(paths)
	client.opLock.Unlock()
	return
}

// Remove an index.
func (client *BinProtClient) Unindex(colName string, idxPath []string) error {
	return client.reqMaintAccess(func() error {
		for i := 0; i < client.nProcs; i++ {
			if clientDB, err := db.OpenDB(path.Join(client.workspace, strconv.Itoa(i))); err != nil {
				return err
			} else if clientDB.Use(colName) == nil {
				continue
			} else if err = clientDB.Use(colName).Unindex(idxPath); err != nil {
				return err
			} else if err = clientDB.Close(); err != nil {
				return err
			}
		}
		return nil
	})
}
