package db

import (
	"encoding/json"
	"fmt"
	"loveoneanother.at/tiedot/uid"
	"os"
	"testing"
)

func ensureMapHasKeys(m map[uint64]struct{}, keys ...uint64) bool {
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

func runQuery(query string, col *Col) (map[uint64]struct{}, error) {
	result := make(map[uint64]struct{})
	var jq interface{}
	if err := json.Unmarshal([]byte(query), &jq); err != nil {
		fmt.Println(err)
	}
	return result, EvalQuery(jq, col, &result)
}

func TestQuery(t *testing.T) {
	// prepare a collection of documents
	tmp := "/tmp/tiedot_query_test"
	os.RemoveAll(tmp)
	defer os.RemoveAll(tmp)
	col, err := OpenCol(tmp, uid.MiniUIDPool())
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
	// hash scan
	q, err := runQuery(`["=", {"eq": 1, "in": ["a", "b"]}]`, col)
	if err != nil {
		t.Fatal(err)
	}
	if !ensureMapHasKeys(q, ids[0], ids[1]) {
		t.Fatal(q)
	}
	q, err = runQuery(`["=", {"eq": 1, "limit": 1, "in": ["a", "b"]}]`, col)
	if err != nil {
		fmt.Println(err)
	}
	if !ensureMapHasKeys(q, ids[0]) {
		t.Fatal(q)
	}
	// collection scan
	q, err = runQuery(`["=", {"eq": 1, "in": ["c"]}]`, col)
	if err != nil {
		t.Fatal(err)
	}
	if !ensureMapHasKeys(q, ids[0], ids[1]) {
		t.Fatal(q)
	}
	q, err = runQuery(`["=", {"eq": 1, "limit": 1, "in": ["c"]}]`, col)
	if err != nil {
		t.Fatal(err)
	}
	if !ensureMapHasKeys(q, ids[0]) {
		t.Fatal(q)
	}
	// all documents
	q, err = runQuery(`["all"]`, col)
	if err != nil {
		t.Fatal(err)
	}
	if !ensureMapHasKeys(q, ids[0], ids[1], ids[2], ids[3], ids[4]) {
		t.Fatal(q)
	}
	// union
	q, err = runQuery(`["u", ["=", {"eq": 4, "limit": 1, "in": ["a", "b"]}], ["=", {"eq": 1, "limit": 1, "in": ["c"]}]]`, col)
	if err != nil {
		t.Fatal(err)
	}
	if !ensureMapHasKeys(q, ids[0], ids[4]) {
		t.Fatal(q)
	}
	// intersection
	q, err = runQuery(`["n", ["=", {"eq": 2, "in": ["d"]}], ["all"]]`, col)
	if err != nil {
		t.Fatal(err)
	}
	if !ensureMapHasKeys(q, ids[1], ids[3]) {
		t.Fatal(q)
	}
	// complement
	q, err = runQuery(`["c", ["=", {"eq": 4,  "in": ["c"]}], ["=", {"eq": 2, "in": ["d"]}], ["all"]]`, col)
	if err != nil {
		t.Fatal(err)
	}
	if !ensureMapHasKeys(q, ids[0], ids[2]) {
		t.Fatal(q)
	}
	// lookup on "special"
	q, err = runQuery(`["=", {"eq": {"thing": null},  "in": ["special"]}]`, col)
	if err != nil {
		t.Fatal(err)
	}
	if !ensureMapHasKeys(q, ids[0]) {
		t.Fatal(q)
	}
	// "e" should not exist
	q, err = runQuery(`["exist", {"in": ["e"]}]`, col)
	if err != nil {
		t.Fatal(err)
	}
	if !ensureMapHasKeys(q) {
		t.Fatal(q)
	}
	// existence test, hash scan, with limit
	q, err = runQuery(`["exist", {"in": ["a", "b"], "limit": 3}]`, col)
	if err != nil {
		t.Fatal(err)
	}
	if !ensureMapHasKeys(q, ids[0], ids[1], ids[2]) {
		t.Fatal(q)
	}
	// existence test, collection scan, with limit
	q, err = runQuery(`["exist", {"in": ["c"], "limit": 2}]`, col)
	if err != nil {
		t.Fatal(err)
	}
	if !ensureMapHasKeys(q, ids[0], ids[1]) {
		t.Fatal(q)
	}
}
