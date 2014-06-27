package main

import (
	"encoding/json"
	"fmt"
	"github.com/HouzuoGuo/tiedot/db"
	"os"
)

/*
You are encouraged to use nearly all tiedot public functions concurrently, except schema management functions:
- Create, rename, drop collection
- Create and remove collection index
You may not manage schema and do document operations (such as updates/queries) at the same time.

The embeddedExample requires as much as 1.5GB of free disk space (however not all 1.5 GB are used).

To compile and run the example:
    go build && ./tiedot -mode=example
*/

func embeddedExample() {
	// ****************** Collection Management ******************

	myDBDir := "/tmp/MyDatabase"
	os.RemoveAll(myDBDir)
	defer os.RemoveAll(myDBDir)

	// (Create if not exist) open a database
	myDB, err := db.OpenDB(myDBDir)
	if err != nil {
		panic(err)
	}

	// Create two collections: Feeds and Votes
	if err := myDB.Create("Feeds"); err != nil {
		panic(err)
	}
	if err := myDB.Create("Votes"); err != nil {
		panic(err)
	}

	// What collections do I now have?
	for _, name := range myDB.AllCols() {
		fmt.Printf("I have a collection called %s\n", name)
	}

	// Rename collection "Votes" to "Points"
	if err := myDB.Rename("Votes", "Points"); err != nil {
		panic(err)
	}

	// Drop (delete) collection "Points"
	if err := myDB.Drop("Points"); err != nil {
		panic(err)
	}

	// Scrub (repair and compact) "Feeds"
	myDB.Scrub("Feeds")

	// ****************** Document Management ******************
	// Start using a collection
	// (The reference is valid until DB schema changes or Scrub is carried out)
	feeds := myDB.Use("Feeds")

	// Insert document (document must be map[string]interface{})
	docID, err := feeds.Insert(map[string]interface{}{
		"name": "Go 1.2 is released",
		"url":  "golang.org"})
	if err != nil {
		panic(err)
	}

	// Read document
	readBack, err := feeds.Read(docID)
	fmt.Println(readBack)

	// Update document (document must be map[string]interface{})
	err = feeds.Update(docID, map[string]interface{}{
		"name": "Go is very popular",
		"url":  "google.com"})
	if err != nil {
		panic(err)
	}

	// Delete document
	feeds.Delete(docID)

	// ****************** Index Management ******************
	// Secondary indexes assist in many types of queries
	// Create index (path leads to document JSON attribute)
	if err := feeds.Index([]string{"author", "name", "first_name"}); err != nil {
		panic(err)
	}
	if err := feeds.Index([]string{"Title"}); err != nil {
		panic(err)
	}
	if err := feeds.Index([]string{"Source"}); err != nil {
		panic(err)
	}

	// What indexes do I have on collection A?
	for _, path := range feeds.AllIndexes() {
		fmt.Printf("I have an index on path %v\n", path)
	}

	// Remove index
	if err := feeds.Unindex([]string{"author", "name", "first_name"}); err != nil {
		panic(err)
	}

	// ****************** Queries ******************
	// Prepare some documents for the query
	feeds.Insert(map[string]interface{}{"Title": "New Go release", "Source": "golang.org", "Age": 3})
	feeds.Insert(map[string]interface{}{"Title": "Kitkat is here", "Source": "google.com", "Age": 2})
	feeds.Insert(map[string]interface{}{"Title": "Good Slackware", "Source": "slackware.com", "Age": 1})

	var query interface{}
	json.Unmarshal([]byte(`[{"eq": "New Go release", "in": ["Title"]}, {"eq": "slackware.com", "in": ["Source"]}]`), &query)

	queryResult := make(map[int]struct{}) // query result (document IDs) goes into map keys

	if err := db.EvalQuery(query, feeds, &queryResult); err != nil {
		panic(err)
	}

	// Query results are document IDs
	for id := range queryResult {
		fmt.Printf("Query returned document ID %d\n", id)
	}

	// To use the document itself, simply read it back
	for id := range queryResult {
		readBack, err := feeds.Read(id)
		if err != nil {
			panic(err)
		}
		fmt.Printf("Query returned document %v\n", readBack)
	}

	// Make sure important transactions are persisted (very expensive call: do NOT invoke too often)
	// (A background goroutine is already doing it for you every few seconds)
	if err := myDB.Sync(); err != nil {
		panic(err)
	}
	// Gracefully close database, you should call Close on all opened databases
	// Otherwise background goroutines will prevent program shutdown
	if err := myDB.Close(); err != nil {
		panic(err)
	}
}
