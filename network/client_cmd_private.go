/* tiedot command implementations - client side private APIs - for testing purpose only. */
package network

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// Put a key-value pair into hash table (no corresponding public API).
func (tc *Client) htPut(colName, indexName string, key, val uint64) (err error) {
	rank := int(key % uint64(tc.TotalRank))
	mutex := tc.mutex[rank]
	mutex.Lock()
	err = tc.getOK(rank, fmt.Sprintf("%s %s %s %d %d", HT_PUT, colName, indexName, key, val))
	mutex.Unlock()
	return
}

// Put a key-value pair into hash table (no corresponding public API).
func (tc *Client) htGet(colName, indexName string, key, limit uint64) (vals []uint64, err error) {
	rank := int(key % uint64(tc.TotalRank))
	mutex := tc.mutex[rank]
	mutex.Lock()
	resp, err := tc.getStr(rank, fmt.Sprintf("%s %s %s %d %d", HT_GET, colName, indexName, key, limit))
	mutex.Unlock()
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
func (tc *Client) htDelete(colName, indexName string, key, val uint64) (err error) {
	rank := int(key % uint64(tc.TotalRank))
	mutex := tc.mutex[rank]
	mutex.Lock()
	err = tc.getOK(rank, fmt.Sprintf("%s %s %s %d %d", HT_DELETE, colName, indexName, key, val))
	mutex.Unlock()
	return
}
