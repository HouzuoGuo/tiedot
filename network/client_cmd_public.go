/* tiedot command implementations - client side, public APIs. */
package network

import "fmt"

// Tell server to shutdown, and shutdown myself (client) as well.
func (tc *Client) ShutdownServer() {
	tc.writeReq(SHUTDOWN)
	tc.ShutdownClient()
}

// Create a collection.
func (tc *Client) ColCreate(name string, numParts int) error {
	return tc.getOK(fmt.Sprintf("%s %s %d", COL_CREATE, name, numParts))
}

// Get all collection information (collection name VS number of partitions).
func (tc *Client) ColAll() (all map[string]int, err error) {
	all = make(map[string]int)
	js, err := tc.getJSON(COL_ALL)
	if err != nil {
		return
	}
	for name, numParts := range js.(map[string]interface{}) {
		all[name] = int(numParts.(float64))
	}
	return
}

// Rename a collection.
func (tc *Client) ColRename(oldName, newName string) error {
	return tc.getOK(fmt.Sprintf("%s %s %s", COL_RENAME, oldName, newName))
}

// Drop a collection.
func (tc *Client) ColDrop(colName string) error {
	return tc.getOK(fmt.Sprintf("%s %s", COL_DROP, colName))
}
