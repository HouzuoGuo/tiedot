package dbsvc

import (
	"encoding/json"
	"fmt"
	"github.com/HouzuoGuo/tiedot/datasvc"
	"testing"
)

func StrHashTest(t *testing.T) {
	strings := []string{"", " ", "abc", "123"}
	hashes := []int{0, 32, 417419622498, 210861491250}
	for i := range strings {
		if StrHash(strings[i]) != hashes[i] {
			t.Fatalf("Hash of %s equals to %d, it should equal to %d", strings[i], StrHash(strings[i]), hashes[i])
		}
	}
}

func GetInTest(t *testing.T) {
	var obj interface{}
	// Get inside a JSON object
	json.Unmarshal([]byte(`{"a": {"b": {"c": 1}}}`), &obj)
	if val, ok := GetIn(obj, []string{"a", "b", "c"})[0].(float64); !ok || val != 1 {
		t.Fatal()
	}
	// Get inside a JSON array
	json.Unmarshal([]byte(`{"a": {"b": {"c": [1, 2, 3]}}}`), &obj)
	if val, ok := GetIn(obj, []string{"a", "b", "c"})[0].(float64); !ok || val != 1 {
		t.Fatal()
	}
	if val, ok := GetIn(obj, []string{"a", "b", "c"})[1].(float64); !ok || val != 2 {
		t.Fatal()
	}
	if val, ok := GetIn(obj, []string{"a", "b", "c"})[2].(float64); !ok || val != 3 {
		t.Fatal()
	}
	// Get inside JSON objects contained in JSON array
	json.Unmarshal([]byte(`{"a": [{"b": {"c": [1]}}, {"b": {"c": [2, 3]}}]}`), &obj)
	if val, ok := GetIn(obj, []string{"a", "b", "c"})[0].(float64); !ok || val != 1 {
		t.Fatal()
	}
	if val, ok := GetIn(obj, []string{"a", "b", "c"})[1].(float64); !ok || val != 2 {
		t.Fatal()
	}
	if val, ok := GetIn(obj, []string{"a", "b", "c"})[2].(float64); !ok || val != 3 {
		t.Fatal()
	}
	// Get inside a JSON array and fetch attributes from array elements, which are JSON objects
	json.Unmarshal([]byte(`{"a": [{"b": {"c": [4]}}, {"b": {"c": [5, 6]}}], "d": [0, 9]}`), &obj)
	if val, ok := GetIn(obj, []string{"a", "b", "c"})[0].(float64); !ok || val != 4 {
		t.Fatal()
	}
	if val, ok := GetIn(obj, []string{"a", "b", "c"})[1].(float64); !ok || val != 5 {
		t.Fatal()
	}
	if val, ok := GetIn(obj, []string{"a", "b", "c"})[2].(float64); !ok || val != 6 {
		t.Fatal()
	}
	if len(GetIn(obj, []string{"a", "b", "c"})) != 3 {
		t.Fatal()
	}
	if val, ok := GetIn(obj, []string{"d"})[0].(float64); !ok || val != 0 {
		t.Fatal()
	}
	if val, ok := GetIn(obj, []string{"d"})[1].(float64); !ok || val != 9 {
		t.Fatal()
	}
	if len(GetIn(obj, []string{"d"})) != 2 {
		t.Fatal()
	}
	// Another example
	json.Unmarshal([]byte(`{"a": {"b": [{"c": 2}]}, "d": 0}`), &obj)
	if val, ok := GetIn(obj, []string{"a", "b", "c"})[0].(float64); !ok || val != 2 {
		t.Fatal()
	}
	if len(GetIn(obj, []string{"a", "b", "c"})) != 1 {
		t.Fatal()
	}
}

func idxHas(t *testing.T, colName string, path []string, idxVal interface{}, docID int) error {
	idxName := mkIndexUID(colName, path)
	var out []int
	hashKey := StrHash(fmt.Sprint(idxVal))
	if err := db.data[hashKey%db.totalRank].Call("DataSvc.HTGet", datasvc.HTGetInput{idxName, hashKey, 0, db.mySchemaVersion}, &out); err != nil {
		panic(err)
	}
	if len(out) != 1 || out[0] != docID {
		return fmt.Errorf("Looking for %v %v %v in %v, but got result %v", idxVal, hashKey, docID, path, out)
	}
	return nil
}

func idxHasNot(t *testing.T, colName string, path []string, idxVal, docID int) error {
	idxName := mkIndexUID(colName, path)
	var out []int
	hashKey := StrHash(fmt.Sprint(idxVal))
	if err := db.data[hashKey%db.totalRank].Call("DataSvc.HTGet", datasvc.HTGetInput{idxName, hashKey, 0, db.mySchemaVersion}, &out); err != nil {
		panic(err)
	}
	for _, v := range out {
		if v == docID {
			return fmt.Errorf("Looking for %v %v %v in %v (should not return any), but got result %v", idxVal, hashKey, docID, path, out)
		}
	}
	return nil
}

func DocCrudAndIndexTest(t *testing.T) {
	var err error
	if err = db.ColCreate("DocCrudTest"); err != nil {
		t.Fatal(err)
	}
	if err = db.IdxCreate("DocCrudTest", []string{"a", "b"}); err != nil {
		t.Fatal(err)
	}
	numDocs := 100
	docIDs := make([]int, numDocs)
	// Insert documents
	if docIDs[0], err = db.DocInsert("reioavd", map[string]interface{}{}); err == nil {
		t.Fatal("Did not error")
	}
	for i := 0; i < numDocs; i++ {
		if i%40 == 0 {
			if err = db.Sync(); err != nil {
				t.Fatal(err)
			}
		}
		if docIDs[i], err = db.DocInsert("DocCrudTest", map[string]interface{}{"a": map[string]interface{}{"b": i}}); err != nil {
			t.Fatal(err)
		}
	}
	// Read documents
	if _, err := db.DocRead("awefd", docIDs[0]); err == nil {
		t.Fatal("Did not error")
	}
	if _, err := db.DocRead("DocCrudTest", 912345); err == nil {
		t.Fatal("Did not error")
	}
	for i := 0; i < numDocs; i++ {
		if i%40 == 0 {
			if err = db.Sync(); err != nil {
				t.Fatal(err)
			}
		}
		if doc, err := db.DocRead("DocCrudTest", docIDs[i]); err != nil || doc["a"].(map[string]interface{})["b"].(float64) != float64(i) {
			t.Fatal(doc, err)
		}
		if err = idxHas(t, "DocCrudTest", []string{"a", "b"}, i, docIDs[i]); err != nil {
			t.Fatal(err)
		}
	}
	// Update documents
	if err = db.DocUpdate("awnboin", docIDs[0], map[string]interface{}{}); err == nil {
		t.Fatal("Did not error")
	}
	if err = db.DocUpdate("awnboin", 987324, map[string]interface{}{}); err == nil {
		t.Fatal("Did not error")
	}
	for i := 0; i < numDocs; i++ {
		if i%40 == 0 {
			if err = db.Sync(); err != nil {
				t.Fatal(err)
			}
		}
		// i -> i * 2
		if err = db.DocUpdate("DocCrudTest", docIDs[i], map[string]interface{}{"a": map[string]interface{}{"b": i * 2}}); err != nil {
			t.Fatal(err)
		}
	}
	// Update - verify read back
	for i := 0; i < numDocs; i++ {
		if doc, err := db.DocRead("DocCrudTest", docIDs[i]); err != nil || doc["a"].(map[string]interface{})["b"].(float64) != float64(i*2) {
			t.Fatal(doc, err)
		}
		if i == 0 {
			if err = idxHas(t, "DocCrudTest", []string{"a", "b"}, 0, docIDs[i]); err != nil {
				t.Fatal(err)
			}
		} else {
			if err = idxHasNot(t, "DocCrudTest", []string{"a", "b"}, i, docIDs[i]); err != nil {
				t.Fatal(err)
			}
			if err = idxHas(t, "DocCrudTest", []string{"a", "b"}, i*2, docIDs[i]); err != nil {
				t.Fatal(err)
			}
		}
	}
	// Delete document
	if err := db.DocDelete("aoebnionof", docIDs[1]); err == nil {
		t.Fatal("Did not error")
	}
	if err := db.DocDelete("aoebnionof", 9287347); err == nil {
		t.Fatal("Did not error")
	}
	for i := 0; i < numDocs/2+1; i++ {
		if i%40 == 0 {
			if err = db.Sync(); err != nil {
				t.Fatal(err)
			}
		}
		if err := db.DocDelete("DocCrudTest", docIDs[i]); err != nil {
			t.Fatal(err)
		}
		if err := db.DocDelete("DocCrudTest", docIDs[i]); err == nil {
			t.Fatal("Did not error")
		}
	}
	// Delete - verify read back
	for i := 0; i < numDocs/2+1; i++ {
		if i%40 == 0 {
			if err = db.Sync(); err != nil {
				t.Fatal(err)
			}
		}
		if _, err = db.DocRead("DocCrudTest", docIDs[i]); err == nil {
			t.Fatal("Did not error")
		}
		if err = idxHasNot(t, "DocCrudTest", []string{"a", "b"}, i*2, docIDs[i]); err != nil {
			t.Fatal(err)
		}
	}
	// Delete - verify unaffected docs
	for i := numDocs/2 + 1; i < numDocs; i++ {
		if doc, err := db.DocRead("DocCrudTest", docIDs[i]); err != nil || doc["a"].(map[string]interface{})["b"].(float64) != float64(i*2) {
			t.Fatal(doc, err)
		}
		if err = idxHas(t, "DocCrudTest", []string{"a", "b"}, i*2, docIDs[i]); err != nil {
			t.Fatal(err)
		}
	}
	// Scrub and verify unaffected docs
	if err = db.ColScrub("DocCrudTest"); err != nil {
		t.Fatal(err)
	}
	for i := numDocs/2 + 1; i < numDocs; i++ {
		if doc, err := db.DocRead("DocCrudTest", docIDs[i]); err != nil || doc["a"].(map[string]interface{})["b"].(float64) != float64(i*2) {
			t.Fatal(doc, err)
		}
		if err = idxHas(t, "DocCrudTest", []string{"a", "b"}, i*2, docIDs[i]); err != nil {
			t.Fatal(err)
		}
	}
	// Recreate index and verify
	if err = db.IdxDrop("DocCrudTest", []string{"a", "b"}); err != nil {
		t.Fatal(err)
	}
	if err = db.IdxCreate("DocCrudTest", []string{"a", "b"}); err != nil {
		t.Fatal(err)
	}
	for i := numDocs/2 + 1; i < numDocs; i++ {
		if doc, err := db.DocRead("DocCrudTest", docIDs[i]); err != nil || doc["a"].(map[string]interface{})["b"].(float64) != float64(i*2) {
			t.Fatal(doc, err)
		}
		if err = idxHas(t, "DocCrudTest", []string{"a", "b"}, i*2, docIDs[i]); err != nil {
			t.Fatal(err)
		}
	}
	if err = db.ColDrop("DocCrudTest"); err != nil {
		t.Fatal(err)
	}
}
