package db

import (
	"encoding/json"
	"fmt"
	"os"
	"testing"
)

func ensureMapHasKeys(m map[uint64]bool, keys ...uint64) bool {
	if len(m) != len(keys) {
		return false
	}
	for _, v := range keys {
		if _, ok := m[v]; !ok {
			return false
		}
	}
	return true
}

func runQuery(query string, col *Col) (map[uint64]bool, error) {
	result := make(map[uint64]bool)
	var jq interface{}
	json.Unmarshal([]byte(query), &jq)
	return result, EvalQuery(jq, col, &result)
}

func TestQuery(t *testing.T) {
	// prepare a collection of documents
	tmp := "/tmp/tiedot_query_test"
	os.RemoveAll(tmp)
	defer os.RemoveAll(tmp)
	col, err := OpenCol(tmp)
	if err != nil {
		t.Error(err)
		return
	}
	docs := []string{
		`{"a": {"b": [1]}, "c": 1, "d": 1, "special": {"thing": null} }`,
		`{"a": {"b": 1}, "c": [1], "d": 2}`,
		`{"a": {"b": [2]}, "c": 2, "d": 1}`,
		`{"a": {"b": 3}, "c": [3], "d": 2}`,
		`{"a": {"b": [4]}, "c": 4, "d": 1}`}
	ids := [5]uint64{}
	for i, doc := range docs {
		var jsonDoc interface{}
		json.Unmarshal([]byte(doc), &jsonDoc)
		if ids[i], err = col.Insert(jsonDoc); err != nil {
			fmt.Println(err)
			return
		}
	}
	col.Index([]string{"a", "b"})
	// hash scan
	q, err := runQuery(`["=", {"eq": 1, "in": ["a", "b"]}]`, col)
	if err != nil {
		t.Error(err)
	}
	if !ensureMapHasKeys(q, ids[0], ids[1]) {
		t.Error(q)
	}
	q, err = runQuery(`["=", {"eq": 1, "limit": 1, "in": ["a", "b"]}]`, col)
	if err != nil {
		fmt.Println(err)
	}
	if !ensureMapHasKeys(q, ids[0]) {
		t.Error(q)
	}
	// collection scan
	q, err = runQuery(`["=", {"eq": 1, "in": ["c"]}]`, col)
	if err != nil {
		t.Error(err)
	}
	if !ensureMapHasKeys(q, ids[0], ids[1]) {
		t.Error(q)
	}
	q, err = runQuery(`["=", {"eq": 1, "limit": 1, "in": ["c"]}]`, col)
	if err != nil {
		t.Error(err)
	}
	if !ensureMapHasKeys(q, ids[0]) {
		t.Error(q)
	}
	// all documents
	q, err = runQuery(`["all"]`, col)
	if err != nil {
		t.Error(err)
	}
	if !ensureMapHasKeys(q, ids[0], ids[1], ids[2], ids[3], ids[4]) {
		t.Error(q)
	}
	// union
	q, err = runQuery(`["u", ["=", {"eq": 4, "limit": 1, "in": ["a", "b"]}], ["=", {"eq": 1, "limit": 1, "in": ["c"]}]]`, col)
	if err != nil {
		t.Error(err)
	}
	if !ensureMapHasKeys(q, ids[0], ids[4]) {
		t.Error(q)
	}
	// intersection
	q, err = runQuery(`["n", ["=", {"eq": 2, "in": ["d"]}], ["all"]]`, col)
	if err != nil {
		t.Error(err)
	}
	if !ensureMapHasKeys(q, ids[1], ids[3]) {
		t.Error(q)
	}
	// complement
	q, err = runQuery(`["\\", ["=", {"eq": 4,  "in": ["c"]}], ["=", {"eq": 2, "in": ["d"]}], ["all"]]`, col)
	if err != nil {
		t.Error(err)
	}
	if !ensureMapHasKeys(q, ids[0], ids[2]) {
		t.Error(q)
	}
	// lookup on "special"
	q, err = runQuery(`["=", {"eq": {"thing": null},  "in": ["special"]}]`, col)
	if err != nil {
		t.Error(err)
	}
	if !ensureMapHasKeys(q, ids[0]) {
		t.Error(q)
	}
	col.Close()
}
