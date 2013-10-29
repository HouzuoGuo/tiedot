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
		`{"a": {"b": [1]}, "c": 1, "d": 1, "f": 1, "g": 1, "special": {"thing": null} }`,
		`{"a": {"b": 1}, "c": [1], "d": 2, "f": 2, "g": 2}`,
		`{"a": {"b": [2]}, "c": 2, "d": 1, "f": 3, "g": 3}`,
		`{"a": {"b": 3}, "c": [3], "d": 2, "f": 4, "g": 4}`,
		`{"a": {"b": [4]}, "c": 4, "d": 1, "f": 5, "g": 5}`}
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
	col.Index([]string{"f"})
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
	// all documents
	q, err = runQueryV2(`"all"`, col)
	if err != nil {
		t.Fatal(err)
	}
	if !ensureMapHasKeys(q, ids[0], ids[1], ids[2], ids[3], ids[4]) {
		t.Fatal(q)
	}
	// union
	q, err = runQueryV2(`[{"eq": 4, "limit": 1, "in": ["a", "b"]}, {"eq": 1, "limit": 1, "in": ["c"]}]`, col)
	if err != nil {
		t.Fatal(err)
	}
	if !ensureMapHasKeys(q, ids[0], ids[4]) {
		t.Fatal(q)
	}
	// intersection
	q, err = runQueryV2(`{"n": [{"eq": 2, "in": ["d"]}, "all"]}`, col)
	if err != nil {
		t.Fatal(err)
	}
	if !ensureMapHasKeys(q, ids[1], ids[3]) {
		t.Fatal(q)
	}
	// complement
	q, err = runQueryV2(`{"c": [{"eq": 4,  "in": ["c"]}, {"eq": 2, "in": ["d"]}, "all"]}`, col)
	if err != nil {
		t.Fatal(err)
	}
	if !ensureMapHasKeys(q, ids[0], ids[2]) {
		t.Fatal(q)
	}
	// lookup on "special"
	q, err = runQueryV2(`{"eq": {"thing": null},  "in": ["special"]}`, col)
	if err != nil {
		t.Fatal(err)
	}
	if !ensureMapHasKeys(q, ids[0]) {
		t.Fatal(q)
	}
	// "e" should not exist
	q, err = runQueryV2(`{"has": ["e"]}`, col)
	if err != nil {
		t.Fatal(err)
	}
	if !ensureMapHasKeys(q) {
		t.Fatal(q)
	}
	// existence test, hash scan, with limit
	q, err = runQueryV2(`{"has": ["a", "b"], "limit": 3}`, col)
	if err != nil {
		t.Fatal(err)
	}
	if !ensureMapHasKeys(q, ids[0], ids[1], ids[2]) {
		t.Fatal(q)
	}
	// existence test, collection scan, with limit
	q, err = runQueryV2(`{"has": ["c"], "limit": 2}`, col)
	if err != nil {
		t.Fatal(err)
	}
	if !ensureMapHasKeys(q, ids[0], ids[1]) {
		t.Fatal(q)
	}
	// int hash scan
	q, err = runQueryV2(`{"int-from": 2, "int-to": 4, "in": ["f"]}`, col)
	if err != nil {
		t.Fatal(err)
	}
	if !ensureMapHasKeys(q, ids[1], ids[2], ids[3]) {
		t.Fatal(q)
	}
	q, err = runQueryV2(`{"int-from": 2, "int-to": 4, "in": ["f"], "limit": 2}`, col)
	if err != nil {
		t.Fatal(err)
	}
	if !ensureMapHasKeys(q, ids[1], ids[2]) {
		t.Fatal(q)
	}
	// int collection scan
	q, err = runQueryV2(`{"int-from": 2, "int-to": 4, "in": ["g"]}`, col)
	if err != nil {
		t.Fatal(err)
	}
	if !ensureMapHasKeys(q, ids[1], ids[2], ids[3]) {
		t.Fatal(q)
	}
	q, err = runQueryV2(`{"int-from": 2, "int-to": 4, "in": ["g"], "limit": 2}`, col)
	if err != nil {
		t.Fatal(err)
	}
	if !ensureMapHasKeys(q, ids[1], ids[2]) {
		t.Fatal(q)
	}
	// int collection scan with reversed range and limit
	q, err = runQueryV2(`{"int-from": 10, "int-to": 0, "in": ["f"]}`, col)
	if err != nil {
		t.Fatal(err)
	}
	if !ensureMapHasKeys(q, ids[4], ids[3], ids[2], ids[1], ids[0]) {
		t.Fatal(q)
	}
	q, err = runQueryV2(`{"int-from": 10, "int-to": 0, "in": ["f"], "limit": 2}`, col)
	if err != nil {
		t.Fatal(err)
	}
	if !ensureMapHasKeys(q, ids[4], ids[3]) {
		t.Fatal(q)
	}
}
