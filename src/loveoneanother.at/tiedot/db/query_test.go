package db

/*

import (
	"encoding/json"
	"os"
	"testing"
)

func Test(t *testing.T) {
	// prepare a collection of documents
	tmp := "/tmp/tiedot_col_test"
	os.RemoveAll(tmp)
	defer os.RemoveAll(tmp)
	col, err := OpenCol(tmp)
	docs := []string{
		`{"a": {"b": [1]}, "c": 1}`,
		`{"a": {"b": 2}, "c": 2}`,
		`{"a": {"b": 3}, "c": [3]}`,
		`{"a": {"b": 4}, "c": 4}`,
		`{"a": {"b": 5}, "c": 5}`}
	for _, doc := range docs {
		var jsonDoc interface{}
		json.Unmarshal([]byte(doc), &jsonDoc)
		if _, err := col.Insert(jsonDoc); err != nil {
			t.Error(err)
			return
		}
	}
	col.Index([]string{"a", "b"})
	// query tests begin
	result := make(map[uint64]bool)
	query := `["=", {"eq": [1], limit `
	// hash scan
	result = make(map[uint64]bool)
	query = ``
	// collection scan
	result = make(map[uint64]bool)
	query = ``
	// all documents
	result = make(map[uint64]bool)
	query = ``
	// union
	result = make(map[uint64]bool)
	query = ``
	// intersection
	result = make(map[uint64]bool)
	query = ``
	// complement
	result = make(map[uint64]bool)
	query = ``
}

*/