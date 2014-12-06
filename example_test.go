package main

import (
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/HouzuoGuo/tiedot/db"
	"github.com/HouzuoGuo/tiedot/dberr"
)

func sameMap(m1 map[string]interface{}, m2 map[string]interface{}) bool {
	if str1, err := json.Marshal(m1); err != nil {
		panic(err)
	} else if str2, err := json.Marshal(m2); err != nil {
		panic(err)
	} else if string(str1) != string(str2) {
		return false
	}
	return true
}

// example.go written in test case style
func TestAll(t *testing.T) {
	myDBDir := "/tmp/tiedot_test_embeddedExample"
	os.RemoveAll(myDBDir)
	defer os.RemoveAll(myDBDir)

	myDB, err := db.OpenDB(myDBDir)
	if err != nil {
		t.Fatal(err)
	}
	if err := myDB.Create("Feeds"); err != nil {
		t.Fatal(err)
	}
	if err := myDB.Create("Votes"); err != nil {
		t.Fatal(err)
	}
	for _, name := range myDB.AllCols() {
		if name != "Feeds" && name != "Votes" {
			t.Fatal(myDB.AllCols())
		}
	}
	if err := myDB.Rename("Votes", "Points"); err != nil {
		t.Fatal(err)
	}
	if err := myDB.Drop("Points"); err != nil {
		t.Fatal(err)
	}
	if err := myDB.Scrub("Feeds"); err != nil {
		t.Fatal(err)
	}

	feeds := myDB.Use("Feeds")
	docID, err := feeds.Insert(map[string]interface{}{
		"name": "Go 1.2 is released",
		"url":  "golang.org"})
	if err != nil {
		t.Fatal(err)
	}
	readBack, err := feeds.Read(docID)
	if err != nil {
		t.Fatal(err)
	}
	if !sameMap(readBack, map[string]interface{}{
		"name": "Go 1.2 is released",
		"url":  "golang.org"}) {
		t.Fatal(readBack)
	}
	err = feeds.Update(docID, map[string]interface{}{
		"name": "Go is very popular",
		"url":  "google.com"})
	if err != nil {
		panic(err)
	}
	feeds.ForEachDoc(func(id int, docContent []byte) (willMoveOn bool) {
		var doc map[string]interface{}
		if json.Unmarshal(docContent, &doc) != nil {
			t.Fatal("cannot deserialize")
		}
		if !sameMap(doc, map[string]interface{}{
			"name": "Go is very popular",
			"url":  "google.com"}) {
			t.Fatal(doc)
		}
		return true
	})
	if err := feeds.Delete(docID); err != nil {
		t.Fatal(err)
	}
	if err := feeds.Delete(docID); dberr.Type(err) != dberr.ErrorNoDoc {
		t.Fatal(err)
	}

	if err := feeds.Index([]string{"author", "name", "first_name"}); err != nil {
		t.Fatal(err)
	}
	if err := feeds.Index([]string{"Title"}); err != nil {
		t.Fatal(err)
	}
	if err := feeds.Index([]string{"Source"}); err != nil {
		t.Fatal(err)
	}
	for _, path := range feeds.AllIndexes() {
		joint := strings.Join(path, "")
		if joint != "authornamefirst_name" && joint != "Title" && joint != "Source" {
			t.Fatal(feeds.AllIndexes())
		}
	}
	if err := feeds.Unindex([]string{"author", "name", "first_name"}); err != nil {
		t.Fatal(err)
	}

	// ****************** Queries ******************
	// Prepare some documents for the query
	if _, err := feeds.Insert(map[string]interface{}{"Title": "New Go release", "Source": "golang.org", "Age": 3}); err != nil {
		t.Fatal("failure")
	}
	if _, err := feeds.Insert(map[string]interface{}{"Title": "Kitkat is here", "Source": "google.com", "Age": 2}); err != nil {
		t.Fatal("failure")
	}
	if _, err := feeds.Insert(map[string]interface{}{"Title": "Good Slackware", "Source": "slackware.com", "Age": 1}); err != nil {
		t.Fatal("failure")
	}
	var query interface{}
	if json.Unmarshal([]byte(`[{"eq": "New Go release", "in": ["Title"]}, {"eq": "slackware.com", "in": ["Source"]}]`), &query) != nil {
		t.Fatal("failure")
	}
	queryResult := make(map[int]struct{}) // query result (document IDs) goes into map keys
	if err := db.EvalQuery(query, feeds, &queryResult); err != nil {
		t.Fatal(err)
	}
	// Query result are document IDs
	for id := range queryResult {
		// To get query result document, simply read it
		readBack, err := feeds.Read(id)
		if err != nil {
			panic(err)
		}
		if !sameMap(readBack, map[string]interface{}{"Title": "New Go release", "Source": "golang.org", "Age": 3}) &&
			!sameMap(readBack, map[string]interface{}{"Title": "Good Slackware", "Source": "slackware.com", "Age": 1}) {
			t.Fatal(readBack)
		}
	}

	if err := myDB.Close(); err != nil {
		t.Fatal(err)
	}
}
