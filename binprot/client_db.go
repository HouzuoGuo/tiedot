// Binary protocol over IPC - DB management features (client).

package binprot

import (
	"github.com/HouzuoGuo/tiedot/db"
	"github.com/HouzuoGuo/tiedot/tdlog"
	"path"
	"sort"
	"strconv"
)

// Create a new collection.
func (client *BinProtClient) Create(colName string) error {
	return client.reqMaintAccess(func() error {
		for i := 0; i < len(client.sock); i++ {
			clientDB, err := db.OpenDB(path.Join(client.workspace, strconv.Itoa(i)))
			if err != nil {
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
		for i := 0; i < len(client.sock); i++ {
			clientDB, err := db.OpenDB(path.Join(client.workspace, strconv.Itoa(i)))
			if err != nil {
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
		for i := 0; i < len(client.sock); i++ {
			clientDB, err := db.OpenDB(path.Join(client.workspace, strconv.Itoa(i)))
			if err != nil {
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

// Drop a collection.
func (client *BinProtClient) Drop(colName string) error {
	return client.reqMaintAccess(func() error {
		for i := 0; i < len(client.sock); i++ {
			clientDB, err := db.OpenDB(path.Join(client.workspace, strconv.Itoa(i)))
			if err != nil {
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
