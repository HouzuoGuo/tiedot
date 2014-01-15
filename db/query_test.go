package db

import (
	"encoding/json"
	"fmt"
	"github.com/HouzuoGuo/tiedot/chunkfile"
	"os"
	"strings"
	"testing"
)

func ensureMapHasKeys(m map[int]struct{}, keys ...int) bool {
	if len(m) != len(keys) {
		return false
	}
	for _, v := range keys {
		if _, ok := m[int(v)]; !ok {
			return false
		}
	}
	return true
}

func runQuery(query string, col *Col) (map[int]struct{}, error) {
	result := make(map[int]struct{})
	var jq interface{}
	if err := json.Unmarshal([]byte(query), &jq); err != nil {
		fmt.Println(err)
	}
	return result, EvalQuery(jq, col, &result)
}

func PaddingAttr(n uint64) string {
	return `"padding": "` + strings.Repeat(" ", int(n)) + `"`
}

func TestQuery(t *testing.T) {
	// prepare a collection of documents
	tmp := "/tmp/tiedot_query_test"
	os.RemoveAll(tmp)
	defer os.RemoveAll(tmp)
	col, err := OpenCol(tmp, 2)
	if err != nil {
		t.Fatal(err)
		return
	}
	defer col.Close()
	docs := []string{
		`{"a": {"b": [1]}, "c": 1, "d": 1, "f": 1, "g": 1, "special": {"thing": null}, "h": 1, ` + PaddingAttr(chunkfile.DOC_MAX_ROOM/3) + `}`,
		`{"a": {"b": 1}, "c": [1], "d": 2, "f": 2, "g": 2, ` + PaddingAttr(chunkfile.DOC_MAX_ROOM/3) + `}`,
		`{"a": [{"b": [2]}], "c": 2, "d": 1, "f": 3, "g": 3, "h": 3, ` + PaddingAttr(chunkfile.DOC_MAX_ROOM/3) + `}`,
		`{"a": {"b": 3}, "c": [3], "d": 2, "f": 4, "g": 4, ` + PaddingAttr(chunkfile.DOC_MAX_ROOM/3) + `}`,
		`{"a": {"b": [4]}, "c": 4, "d": 1, "f": 5, "g": 5, ` + PaddingAttr(chunkfile.DOC_MAX_ROOM/3) + `}`,
		`{"a": [{"b": 5}, {"b": 6}], "c": 4, "d": 1, "f": 5, "g": 5, "h": 2, ` + PaddingAttr(chunkfile.DOC_MAX_ROOM/3) + `}`}
	ids := [6]int{}
	for i, doc := range docs {
		var jsonDoc map[string]interface{}
		json.Unmarshal([]byte(doc), &jsonDoc)
		if ids[i], err = col.Insert(jsonDoc); err != nil {
			fmt.Println(err)
			return
		}
	}
	col.Index([]string{"a", "b"})
	col.Index([]string{"f"})
	col.Index([]string{"h"})
	col.Index([]string{"special"})
	col.Index([]string{"e"})
	// expand numbers
	q, err := runQuery(`["1", "2", ["3", "4"], "5"]`, col)
	if err != nil {
		t.Fatal(err)
	}
	if !ensureMapHasKeys(q, 1, 2, 3, 4, 5) {
		t.Fatal(q)
	}
	// hash scan
	q, err = runQuery(`{"eq": 1, "in": ["a", "b"]}`, col)
	if err != nil {
		t.Fatal(err)
	}
	if !ensureMapHasKeys(q, ids[0], ids[1]) {
		t.Fatal(q)
	}
	q, err = runQuery(`{"eq": 5, "in": ["a", "b"]}`, col)
	if err != nil {
		t.Fatal(err)
	}
	if !ensureMapHasKeys(q, ids[5]) {
		t.Fatal(q)
	}
	q, err = runQuery(`{"eq": 6, "in": ["a", "b"]}`, col)
	if err != nil {
		t.Fatal(err)
	}
	if !ensureMapHasKeys(q, ids[5]) {
		t.Fatal(q)
	}
	q, err = runQuery(`{"eq": 1, "limit": 1, "in": ["a", "b"]}`, col)
	if err != nil {
		fmt.Println(err)
	}
	if !ensureMapHasKeys(q, ids[1]) && !ensureMapHasKeys(q, ids[0]) {
		t.Fatal(q, ids[1], ids[0])
	}
	// collection scan + pk scan
	q, err = runQuery(`{"eq": 1, "in": ["c"]}`, col)
	if err == nil {
		t.Fatal("Collection scan should trigger error")
	}
	q, err = runQuery(`{"eq": 1, "in": ["_pk"]}`, col)
	if err == nil {
		t.Fatal("PK scan should trigger error")
	}
	// lookup on "special"
	q, err = runQuery(`{"eq": {"thing": null},  "in": ["special"]}`, col)
	if err != nil {
		t.Fatal(err)
	}
	if !ensureMapHasKeys(q, ids[0]) {
		t.Fatal(q)
	}
	// "e" should not exist
	q, err = runQuery(`{"has": ["e"]}`, col)
	if err != nil {
		t.Fatal(err)
	}
	if !ensureMapHasKeys(q) {
		t.Fatal(q)
	}
	// existence test, hash scan, with limit
	q, err = runQuery(`{"has": ["h"], "limit": 2}`, col)
	if err != nil {
		t.Fatal(err)
	}
	if !ensureMapHasKeys(q, ids[0], ids[2]) && !ensureMapHasKeys(q, ids[2], ids[5]) && !ensureMapHasKeys(q, ids[5], ids[0]) {
		t.Fatal(q, ids[0], ids[1], ids[2])
	}
	// existence test, collection scan & PK
	q, err = runQuery(`{"has": ["c"], "limit": 2}`, col)
	if err == nil {
		t.Fatal("Existence test should return error")
	}
	q, err = runQuery(`{"has": ["_pk"], "limit": 2}`, col)
	if err == nil {
		t.Fatal("Existence test should return error")
	}
	// int hash scan
	q, err = runQuery(`{"int-from": 2, "int-to": 4, "in": ["f"]}`, col)
	if err != nil {
		t.Fatal(err)
	}
	if !ensureMapHasKeys(q, ids[1], ids[2], ids[3]) {
		t.Fatal(q)
	}
	q, err = runQuery(`{"int-from": 2, "int-to": 4, "in": ["f"], "limit": 2}`, col)
	if err != nil {
		t.Fatal(err)
	}
	if !ensureMapHasKeys(q, ids[1], ids[2]) {
		t.Fatal(q, ids[1], ids[2])
	}
	// int hash scan using reversed range and limit
	q, err = runQuery(`{"int-from": 10, "int-to": 0, "in": ["f"]}`, col)
	if err != nil {
		t.Fatal(err)
	}
	if !ensureMapHasKeys(q, ids[5], ids[4], ids[3], ids[2], ids[1], ids[0]) {
		t.Fatal(q)
	}
	q, err = runQuery(`{"int-from": 10, "int-to": 0, "in": ["f"], "limit": 2}`, col)
	if err != nil {
		t.Fatal(err)
	}
	if !ensureMapHasKeys(q, ids[5], ids[4]) {
		t.Fatal(q)
	}
	// regexes
	q, err = runQuery(`{"re": "^[0-9]*$", "in": ["f"]}`, col)
	if err != nil {
		t.Fatal(err)
	}
	if !ensureMapHasKeys(q, ids[0], ids[1], ids[2], ids[3], ids[4], ids[5]) {
		t.Fatal(q)
	}
	q, err = runQuery(`{"re": ".*", "in": ["a"]}`, col)
	if err != nil {
		t.Fatal(err)
	}
	if !ensureMapHasKeys(q, ids[0], ids[1], ids[2], ids[3], ids[4], ids[5]) {
		fmt.Printf("%+v\n", q)
		t.Fatal(q)
	}
	q, err = runQuery(`{"re": "thing", "in": ["special"]}`, col)
	if err != nil {
		t.Fatal(err)
	}
	if !ensureMapHasKeys(q, ids[0]) {
		fmt.Printf("%+v\n", q)
		t.Fatal(q)
	}
	q, err = runQuery(`{"re": "thing", "in": ["special"]}`, col)
	if err != nil {
		t.Fatal(err)
	}
	if !ensureMapHasKeys(q, ids[0]) {
		fmt.Printf("%+v\n", q)
		t.Fatal(q)
	}
	q, err = runQuery(`{"re": "^[234]$", "in": ["f"], "limit": 2}`, col)
	if err != nil {
		t.Fatal(err)
	}
	if !ensureMapHasKeys(q, ids[1], ids[2]) && !ensureMapHasKeys(q, ids[2], ids[3]) && !ensureMapHasKeys(q, ids[0], ids[2]) {
		fmt.Printf("%+v\n", q)
		t.Fatal(q)
	}
	// all documents
	q, err = runQuery(`"all"`, col)
	if err != nil {
		t.Fatal(err)
	}
	if !ensureMapHasKeys(q, ids[0], ids[1], ids[2], ids[3], ids[4], ids[5]) {
		t.Fatal(q)
	}
	// union
	col.Index([]string{"c"})
	q, err = runQuery(`[{"eq": 4, "limit": 1, "in": ["a", "b"]}, {"eq": 1, "limit": 1, "in": ["c"]}]`, col)
	if err != nil {
		t.Fatal(err)
	}
	if !ensureMapHasKeys(q, ids[0], ids[4]) {
		t.Fatal(q)
	}
	// intersection
	col.Index([]string{"d"})
	q, err = runQuery(`{"n": [{"eq": 2, "in": ["d"]}, "all"]}`, col)
	if err != nil {
		t.Fatal(err)
	}
	if !ensureMapHasKeys(q, ids[1], ids[3]) {
		t.Fatal(q)
	}
	// complement
	q, err = runQuery(`{"c": [{"eq": 4,  "in": ["c"]}, {"eq": 2, "in": ["d"]}, "all"]}`, col)
	if err != nil {
		t.Fatal(err)
	}
	if !ensureMapHasKeys(q, ids[0], ids[2]) {
		t.Fatal(q)
	}
}
