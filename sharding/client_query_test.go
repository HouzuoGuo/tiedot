package sharding

import (
	"encoding/json"
	"fmt"
	"github.com/HouzuoGuo/tiedot/dberr"
	"math/rand"
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

func TestQuery(t *testing.T) {
	var err error
	ws, _, clients := mkServersClients(2)
	defer os.RemoveAll(ws)
	if err := clients[0].Create("col"); err != nil {
		t.Fatal(err)
	}
	// Prepare docs
	docs := []string{
		`{"a": {"b": [1]}, "c": 1, "d": 1, "f": 1, "g": 1, "special": {"thing": null}, "h": 1}`,
		`{"a": {"b": 1}, "c": [1], "d": 2, "f": 2, "g": 2}`,
		`{"a": [{"b": [2]}], "c": 2, "d": 1, "f": 3, "g": 3, "h": 3}`,
		`{"a": {"b": 3}, "c": [3], "d": 2, "f": 4, "g": 4}`,
		`{"a": {"b": [4]}, "c": 4, "d": 1, "f": 5, "g": 5}`,
		`{"a": [{"b": 5}, {"b": 6}], "c": 4, "d": 1, "f": 5, "g": 5, "h": 2}`,
		`{"a": [{"b": "val1"}, {"b": "val2"}]}`,
		`{"a": [{"b": "val3"}, {"b": ["val4", "val5"]}]}`}
	ids := make([]uint64, len(docs))

	// Prepare indexes
	if err := clients[0].Index("col", []string{"a", "b"}); err != nil {
		t.Fatal(err)
	} else if err := clients[1].Index("col", []string{"f"}); err != nil {
		t.Fatal(err)
	}

	// Insert docs
	for i, doc := range docs {
		var jsonDoc map[string]interface{}
		if err := json.Unmarshal([]byte(doc), &jsonDoc); err != nil {
			panic(err)
		}
		if ids[i], err = clients[i%2].Insert("col", jsonDoc); err != nil {
			t.Fatal(err)
		}
	}

	// Prepare more indexes
	if err := clients[0].Index("col", []string{"h"}); err != nil {
		t.Fatal(err)
	} else if err := clients[1].Index("col", []string{"special"}); err != nil {
		t.Fatal(err)
	} else if err := clients[0].Index("col", []string{"e"}); err != nil {
		t.Fatal(err)
	}
	// Prepare test runner function
	runQuery := func(query string) (map[uint64]struct{}, error) {
		result := make(map[uint64]struct{})
		var jq interface{}
		if err := json.Unmarshal([]byte(query), &jq); err != nil {
			fmt.Println(err)
		}
		if err := clients[rand.Intn(2)].EvalQuery(jq, "col", &result); err != nil {
			return nil, err
		}
		return result, nil
	}

	q, err := runQuery(`["all"]`)
	if err != nil {
		t.Fatal(err)
	}
	if !ensureMapHasKeys(q, ids[0], ids[1], ids[2], ids[3], ids[4], ids[5], ids[6], ids[7]) {
		t.Fatal(q)
	}
	// expand numbers
	q, err = runQuery(`["1", "2", ["3", "4"], "5"]`)
	if err != nil {
		t.Fatal(err)
	}
	if !ensureMapHasKeys(q, 1, 2, 3, 4, 5) {
		t.Fatal(q)
	}
	// hash scan
	q, err = runQuery(`{"eq": 1, "in": ["a", "b"]}`)
	if err != nil {
		t.Fatal(err)
	}
	if !ensureMapHasKeys(q, ids[0], ids[1]) {
		t.Fatal(q)
	}
	q, err = runQuery(`{"eq": 5, "in": ["a", "b"]}`)
	if err != nil {
		t.Fatal(err)
	}
	if !ensureMapHasKeys(q, ids[5]) {
		t.Fatal(q)
	}
	q, err = runQuery(`{"eq": 6, "in": ["a", "b"]}`)
	if err != nil {
		t.Fatal(err)
	}
	if !ensureMapHasKeys(q, ids[5]) {
		t.Fatal(q)
	}
	q, err = runQuery(`{"eq": 1, "limit": 1, "in": ["a", "b"]}`)
	if err != nil {
		fmt.Println(err)
	}
	if !ensureMapHasKeys(q, ids[1]) && !ensureMapHasKeys(q, ids[0]) {
		t.Fatal(q, ids[1], ids[0])
	}
	// collection scan
	q, err = runQuery(`{"eq": 1, "in": ["c"]}`)
	if dberr.Type(err) != dberr.ErrorNeedIndex {
		t.Fatal("Collection scan should not happen", err, q)
	}
	// lookup on "special" (null)
	q, err = runQuery(`{"eq": {"thing": null},  "in": ["special"]}`)
	if err != nil {
		t.Fatal(err)
	}
	if !ensureMapHasKeys(q, ids[0]) {
		t.Fatal(q)
	}
	// lookup in list
	q, err = runQuery(`{"eq": "val1",  "in": ["a", "b"]}`)
	if err != nil {
		t.Fatal(err)
	}
	if !ensureMapHasKeys(q, ids[6]) {
		t.Fatal(q)
	}
	q, err = runQuery(`{"eq": "val5",  "in": ["a", "b"]}`)
	if err != nil {
		t.Fatal(err)
	}
	if !ensureMapHasKeys(q, ids[7]) {
		t.Fatal(q)
	}
	// int range scan with incorrect input
	q, err = runQuery(`{"int-from": "a", "int-to": 4, "in": ["f"], "limit": 1}`)
	if dberr.Type(err) != dberr.ErrorExpectingInt {
		t.Fatal(err)
	}
	q, err = runQuery(`{"int-from": 1, "int-to": "a", "in": ["f"], "limit": 1}`)
	if dberr.Type(err) != dberr.ErrorExpectingInt {
		t.Fatal(err)
	}
	q, err = runQuery(`{"int-from": 1, "int-to": 2, "in": ["f"], "limit": "a"}`)
	if dberr.Type(err) != dberr.ErrorExpectingInt {
		t.Fatal(err)
	}
	// int range scan
	q, err = runQuery(`{"int-from": 2, "int-to": 4, "in": ["f"]}`)
	if err != nil {
		t.Fatal(err)
	}
	if !ensureMapHasKeys(q, ids[1], ids[2], ids[3]) {
		t.Fatal(q)
	}
	q, err = runQuery(`{"int-from": 2, "int-to": 4, "in": ["f"], "limit": 2}`)
	if err != nil {
		t.Fatal(err)
	}
	if !ensureMapHasKeys(q, ids[1], ids[2]) {
		t.Fatal(q, ids[1], ids[2])
	}
	// int hash scan using reversed range and limit
	q, err = runQuery(`{"int-from": 10, "int-to": 0, "in": ["f"]}`)
	if err != nil {
		t.Fatal(err)
	}
	if !ensureMapHasKeys(q, ids[5], ids[4], ids[3], ids[2], ids[1], ids[0]) {
		t.Fatal(q)
	}
	q, err = runQuery(`{"int-from": 10, "int-to": 0, "in": ["f"], "limit": 2}`)
	if err != nil {
		t.Fatal(err)
	}
	if !ensureMapHasKeys(q, ids[5], ids[4]) {
		t.Fatal(q)
	}
	// all documents
	q, err = runQuery(`"all"`)
	if err != nil {
		t.Fatal(err)
	}
	if !ensureMapHasKeys(q, ids[0], ids[1], ids[2], ids[3], ids[4], ids[5], ids[6], ids[7]) {
		t.Fatal(q)
	}
	// union
	if err := clients[0].Index("col", []string{"c"}); err != nil {
		t.Fatal(err)
	}
	q, err = runQuery(`[{"eq": 4, "limit": 1, "in": ["a", "b"]}, {"eq": 1, "limit": 1, "in": ["c"]}]`)
	if err != nil {
		t.Fatal(err)
	}
	if !ensureMapHasKeys(q, ids[0], ids[4]) && !ensureMapHasKeys(q, ids[1], ids[4]) {
		t.Fatal(q)
	}
	// intersection
	if err := clients[1].Index("col", []string{"d"}); err != nil {
		t.Fatal(err)
	}
	q, err = runQuery(`{"n": [{"eq": 2, "in": ["d"]}, "all"]}`)
	if err != nil {
		t.Fatal(err)
	}
	if !ensureMapHasKeys(q, ids[1], ids[3]) {
		t.Fatal(q)
	}
	// intersection with incorrect input
	q, err = runQuery(`{"c": null}`)
	if dberr.Type(err) != dberr.ErrorExpectingSubQuery {
		t.Fatal(err)
	}
	// complement
	q, err = runQuery(`{"c": [{"eq": 4,  "in": ["c"]}, {"eq": 2, "in": ["d"]}, "all"]}`)
	if err != nil {
		t.Fatal(err)
	}
	if !ensureMapHasKeys(q, ids[0], ids[2], ids[6], ids[7]) {
		t.Fatal(q)
	}
	// complement with incorrect input
	q, err = runQuery(`{"c": null}`)
	if dberr.Type(err) != dberr.ErrorExpectingSubQuery {
		t.Fatal(err)
	}
	// union of intersection
	q, err = runQuery(`[{"n": [{"eq": 3, "in": ["c"]}]}, {"n": [{"eq": 2, "in": ["c"]}]}]`)
	if !ensureMapHasKeys(q, ids[2], ids[3]) {
		t.Fatal(q)
	}
	// union of complement
	q, err = runQuery(`[{"c": [{"eq": 3, "in": ["c"]}]}, {"c": [{"eq": 2, "in": ["c"]}]}]`)
	if !ensureMapHasKeys(q, ids[2], ids[3]) {
		t.Fatal(q)
	}
	// union of complement of intersection
	q, err = runQuery(`[{"c": [{"n": [{"eq": 1, "in": ["d"]},{"eq": 1, "in": ["c"]}]},{"eq": 1, "in": ["d"]}]},{"eq": 2, "in": ["c"]}]`)
	if !ensureMapHasKeys(q, ids[2], ids[4], ids[5]) {
		t.Fatal(q)
	}

	// If pendingTransaction counter is broken by mistake, server will refuse to go into maintenance mode.
	if _, err = clients[0].goMaintTest(); err != nil {
		t.Fatal(err)
	} else if err = clients[0].leaveMaintTest(); err != nil {
		t.Fatal(err)
	} else if _, err = clients[1].goMaintTest(); err != nil {
		t.Fatal(err)
	} else if err = clients[1].leaveMaintTest(); err != nil {
		t.Fatal(err)
	}
	clients[0].Shutdown()
	clients[1].Shutdown()
}
