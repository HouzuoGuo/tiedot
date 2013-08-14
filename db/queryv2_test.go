package db

import (
	"encoding/json"
	"fmt"
	"os"
	"testing"
)

func runQueryV2(query string, col *Col) (map[uint64]struct{}, error) {
	result := make(map[uint64]struct{})
	var jq interface{}
	if err := json.Unmarshal([]byte(query), &jq); err != nil {
		fmt.Println(err)
	}
	return result, EvalQueryV2(jq, col, &result)
}

func TestQueryV2(t *testing.T) {
	// prepare a collection of documents
	tmp := "/tmp/tiedot_queryv2_test"
	os.RemoveAll(tmp)
	defer os.RemoveAll(tmp)
	col, err := OpenCol(tmp)
	if err != nil {
		t.Fatal(err)
		return
	}
	defer col.Close()
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
	// expand numbers
	q, err := runQueryV2(`[1, 2, [3, 4], 5]`, col)
	if err != nil {
		t.Fatal(err)
	}
	if !ensureMapHasKeys(q, 1, 2, 3, 4, 5) {
		t.Fatal(q)
	}
	// hash scan
	q, err = runQueryV2(`{"eq": 1, "in": ["a", "b"]}`, col)
	if err != nil {
		t.Fatal(err)
	}
	if !ensureMapHasKeys(q, ids[0], ids[1]) {
		t.Fatal(q)
	}
	q, err = runQueryV2(`{"eq": 1, "limit": 1, "in": ["a", "b"]}`, col)
	if err != nil {
		fmt.Println(err)
	}
	if !ensureMapHasKeys(q, ids[0]) {
		t.Fatal(q)
	}
	// collection scan
	q, err = runQueryV2(`{"eq": 1, "in": ["c"]}`, col)
	if err != nil {
		t.Fatal(err)
	}
	if !ensureMapHasKeys(q, ids[0], ids[1]) {
		t.Fatal(q)
	}
	q, err = runQueryV2(`{"eq": 1, "limit": 1, "in": ["c"]}`, col)
	if err != nil {
		t.Fatal(err)
	}
	if !ensureMapHasKeys(q, ids[0]) {
		t.Fatal(q)
	}
	// existence test, hash scan, with limit
	q, err = runQueryV2(`{"exist": ["a", "b"], "limit": 3}`, col)
	if err != nil {
		t.Fatal(err)
	}
	if !ensureMapHasKeys(q, ids[0], ids[1], ids[2]) {
		t.Fatal(q)
	}
	// existence test, collection scan, with limit
	q, err = runQueryV2(`{"exist": ["c"], "limit": 2}`, col)
	if err != nil {
		t.Fatal(err)
	}
	if !ensureMapHasKeys(q, ids[0], ids[1]) {
		t.Fatal(q)
	}
}
