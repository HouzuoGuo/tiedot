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
func (tc *Client) docInsert(colName string, doc map[string]interface{}) error {
	if js, err := json.Marshal(doc); err != nil {
		return errors.New(fmt.Sprintf("Client cannot serialize structure %v, error: %v", doc, err))
	} else {
		return tc.getOK(fmt.Sprintf("%s %s %s", DOC_INSERT, colName, string(js)))
	}
}

// Get a document by ID. (Use ColGet as the public API).
func (tc *Client) docGet(colName string, uid string) (interface{}, error) {
	return tc.getJSON(fmt.Sprintf("%s %s %s", DOC_GET, colName, uid))
}

// Update a document by ID. (Use ColUpdate as the public API).
func (tc *Client) docUpdate(colName string, uid string, newDoc map[string]interface{}) error {
	if js, err := json.Marshal(newDoc); err != nil {
		return errors.New(fmt.Sprintf("Client cannot serialize structure %v, error: %v", newDoc, err))
	} else {
		return tc.getOK(fmt.Sprintf("%s %s %s %s", DOC_UPDATE, colName, uid, js))
	}
}

// Delete a document by ID. (Use ColDelete as the public API).
func (tc *Client) docDelete(colName string, uid string) error {
	return tc.getOK(fmt.Sprintf("%s %s %s %s", DOC_DELETE, colName, uid))
}

// Put a key-value pair into hash table (no corresponding public API).
func (tc *Client) htPut(colName, indexName string, key, val uint64) error {
	return tc.getOK(fmt.Sprintf("%s %s %s %d %d", HT_PUT, colName, indexName, key, val))
}

// Put a key-value pair into hash table (no corresponding public API).
func (tc *Client) htGet(colName, indexName string, key, limit uint64) (keys, vals []uint64, err error) {
	resp, err := tc.getStr(fmt.Sprintf("%s %s %s %d %d", HT_GET, colName, indexName, key, limit))
	if err != nil {
		return
	}
	// The response looks like "key1 key2 key3 val1 val2 val3 ..." so let us disassemble it
	parts := strings.Split(resp, " ")
	keys = make([]uint64, len(parts)/2)
	vals = make([]uint64, len(parts)/2)
	for i := 0; i < len(parts)/2; i++ {
		if num, err := strconv.ParseUint(parts[i], 10, 64); err != nil {
			panic(err) // should not happen
		} else {
			keys[i] = num
		}
	}
	for i, j := len(parts)/2, 0; i < len(parts); i, j = i+1, j+1 {
		if num, err := strconv.ParseUint(parts[i], 10, 64); err != nil {
			panic(err) // should not happen
		} else {
			vals[j] = num
		}
	}
	return
}

// Put a key-value pair into hash table (no corresponding public API).
func (tc *Client) htDelete(colName, indexName string, key, val uint64) error {
	return tc.getOK(fmt.Sprintf("%s %s %s %d %d", HT_DELETE, colName, indexName, key, val))
}
