/* tiedot command implementations - client side, public APIs. */
package network

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/HouzuoGuo/tiedot/uid"
	"strconv"
)

// Tell server to shutdown (all ranks), then shutdown this client.
func (tc *Client) ShutdownServer() {
	for i := 0; i < tc.TotalRank; i++ {
		tc.getOK(i, SHUTDOWN)
	}
	tc.ShutdownClient()
}

// Create a collection.
func (tc *Client) ColCreate(name string) (err error) {
	for _, mutex := range tc.mutex {
		mutex.Lock()
	}
	err = tc.getOK(0, fmt.Sprintf("%s %s", COL_CREATE, name))
	for _, mutex := range tc.mutex {
		mutex.Unlock()
	}
	return
}

// Get all collection information (collection name VS number of partitions).
func (tc *Client) ColAll() (all map[string]int, err error) {
	all = make(map[string]int)
	for _, mutex := range tc.mutex {
		mutex.Lock()
	}
	js, err := tc.getJSON(0, COL_ALL)
	for _, mutex := range tc.mutex {
		mutex.Unlock()
	}
	if err != nil {
		return
	}
	for name, numParts := range js.(map[string]interface{}) {
		all[name] = int(numParts.(float64))
	}
	return
}

// Rename a collection.
func (tc *Client) ColRename(oldName, newName string) (err error) {
	for _, mutex := range tc.mutex {
		mutex.Lock()
	}
	err = tc.getOK(0, fmt.Sprintf("%s %s %s", COL_RENAME, oldName, newName))
	for _, mutex := range tc.mutex {
		mutex.Unlock()
	}
	return
}

// Drop a collection.
func (tc *Client) ColDrop(colName string) (err error) {
	for _, mutex := range tc.mutex {
		mutex.Lock()
	}
	err = tc.getOK(0, fmt.Sprintf("%s %s", COL_DROP, colName))
	for _, mutex := range tc.mutex {
		mutex.Unlock()
	}
	return
}

// Create an index.
func (tc *Client) IdxCreate(colName, idxPath string) (err error) {
	for _, mutex := range tc.mutex {
		mutex.Lock()
	}
	err = tc.getOK(0, fmt.Sprintf("%s %s %s", IDX_CREATE, colName, idxPath))
	for _, mutex := range tc.mutex {
		mutex.Unlock()
	}
	return
}

// Get all indexed paths.
func (tc *Client) IdxAll(colName string) (paths []string, err error) {
	for _, mutex := range tc.mutex {
		mutex.Lock()
	}
	js, err := tc.getJSON(0, fmt.Sprintf("%s %s", IDX_ALL, colName))
	for _, mutex := range tc.mutex {
		mutex.Unlock()
	}
	if err != nil {
		return
	}
	paths = make([]string, 0, 12)
	for _, path := range js.([]interface{}) {
		paths = append(paths, path.(string))
	}
	return paths, nil
}

// Drop an index.
func (tc *Client) IdxDrop(colName, idxPath string) (err error) {
	for _, mutex := range tc.mutex {
		mutex.Lock()
	}
	err = tc.getOK(0, fmt.Sprintf("%s %s %s", IDX_DROP, colName, idxPath))
	for _, mutex := range tc.mutex {
		mutex.Unlock()
	}
	return
}

// Insert a document, return its ID.
func (tc *Client) ColInsert(colName string, js map[string]interface{}) (uint64, error) {
	if js == nil {
		return 0, errors.New("Document is nil")
	}
	id := uid.NextUID()
	js[uid.PK_NAME] = strconv.FormatUint(id, 10)
	rank := int(id % uint64(tc.TotalRank))
	mutex := tc.mutex[rank]
	mutex.Lock()
	if serialized, err := json.Marshal(js); err != nil {
		mutex.Unlock()
		return 0, err
	} else {
		mutex.Unlock()
		return id, tc.getOK(rank, fmt.Sprintf("%s %s %s", COL_INSERT, colName, string(serialized)))
	}
}

// Get a document by ID.
func (tc *Client) ColGet(colName string, id uint64) (doc interface{}, err error) {
	rank := int(id % uint64(tc.TotalRank))
	mutex := tc.mutex[rank]
	mutex.Lock()
	doc, err = tc.getJSON(rank, fmt.Sprintf("%s %s %d", COL_GET, colName, id))
	mutex.Unlock()
	return
}

// Get a document by ID.
func (tc *Client) ColGetJS(colName string, id uint64) (doc string, err error) {
	rank := int(id % uint64(tc.TotalRank))
	mutex := tc.mutex[rank]
	mutex.Lock()
	doc, err = tc.getStr(rank, fmt.Sprintf("%s %s %d", COL_GET, colName, id))
	mutex.Unlock()
	return
}

// Update a document by ID.
func (tc *Client) ColUpdate(colName string, id uint64, js map[string]interface{}) (err error) {
	rank := int(id % uint64(tc.TotalRank))
	mutex := tc.mutex[rank]
	mutex.Lock()
	if serialized, err := json.Marshal(js); err != nil {
		mutex.Unlock()
		return err
	} else {
		mutex.Unlock()
		return tc.getOK(rank, fmt.Sprintf("%s %s %d %s", COL_UPDATE, colName, id, string(serialized)))
	}
}

// Delete a document by ID.
func (tc *Client) ColDelete(colName string, id uint64) (err error) {
	rank := int(id % uint64(tc.TotalRank))
	mutex := tc.mutex[rank]
	mutex.Lock()
	err = tc.getOK(rank, fmt.Sprintf("%s %s %d", COL_DELETE, colName, id))
	mutex.Unlock()
	return
}
