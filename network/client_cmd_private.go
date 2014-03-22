/* tiedot command implementations - client side private APIs - for testing purpose only. */
package network

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// Insert a document. (Use ColInsert as the public API).
func (tc *Client) docInsert(colName string, doc map[string]interface{}) (uint64, error) {
	if js, err := json.Marshal(doc); err != nil {
		return 0, errors.New(fmt.Sprintf("Client cannot serialize structure %v, error: %v", doc, err))
	} else {
		return tc.getUint64(fmt.Sprintf("%s %s %s", DOC_INSERT, colName, string(js)))
	}
}

// Get a document by ID. (Use ColGet as the public API).
func (tc *Client) docGet(colName string, id uint64) (interface{}, error) {
	return tc.getJSON(fmt.Sprintf("%s %s %d", DOC_GET, colName, id))
}

// Update a document by ID. (Use ColUpdate as the public API).
func (tc *Client) docUpdate(colName string, id uint64, newDoc map[string]interface{}) (uint64, error) {
	if js, err := json.Marshal(newDoc); err != nil {
		return 0, errors.New(fmt.Sprintf("Client cannot serialize structure %v, error: %v", newDoc, err))
	} else {
		return tc.getUint64(fmt.Sprintf("%s %s %d %s", DOC_UPDATE, colName, id, js))
	}
}

// Delete a document by ID. (Use ColDelete as the public API).
func (tc *Client) docDelete(colName string, id uint64) error {
	return tc.getOK(fmt.Sprintf("%s %s %d %s", DOC_DELETE, colName, id))
}

// Put a key-value pair into hash table (no corresponding public API).
func (tc *Client) htPut(colName, indexName string, key, val uint64) error {
	return tc.getOK(fmt.Sprintf("%s %s %s %d %d", HT_PUT, colName, indexName, key, val))
}

// Put a key-value pair into hash table (no corresponding public API).
func (tc *Client) htGet(colName, indexName string, key, limit uint64) (vals []uint64, err error) {
	resp, err := tc.getStr(fmt.Sprintf("%s %s %s %d %d", HT_GET, colName, indexName, key, limit))
	if err != nil {
		return
	}
	// The response looks like "val1 val2 val3 ..." so let us disassemble it
	parts := strings.Split(resp, " ")
	vals = make([]uint64, 0, len(parts))
	for i := 0; i < len(parts); i++ {
		if parts[i] == "" {
			continue
		}
		if num, err := strconv.ParseUint(parts[i], 10, 64); err != nil {
			return nil, errors.New(fmt.Sprintf("HTGet client received malformed response from server: %d, %v", len(parts), parts))
		} else {
			vals = append(vals, num)
		}
	}
	return
}

// Put a key-value pair into hash table (no corresponding public API).
func (tc *Client) htDelete(colName, indexName string, key, val uint64) error {
	return tc.getOK(fmt.Sprintf("%s %s %s %d %d", HT_DELETE, colName, indexName, key, val))
}
