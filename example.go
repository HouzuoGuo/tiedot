package main

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/HouzuoGuo/tiedot/db"
	"github.com/HouzuoGuo/tiedot/dberr"
)

/*
In embedded usage, you are encouraged to use all public functions concurrently.
However please do not use public functions in "data" package by yourself - you most likely will not need to use them directly.

To compile and run the example:
    go build && ./tiedot -mode=example

It may require as much as 1.5GB of free disk space in order to run the example.
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
	if err := myDB.Scrub("Feeds"); err != nil {
		panic(err)
	}

	// ****************** Document Management ******************

	// Start using a collection (the reference is valid until DB schema changes or Scrub is carried out)
	feeds := myDB.Use("Feeds")

	// Insert document (afterwards the docID uniquely identifies the document and will never change)
	docID, err := feeds.Insert(map[string]interface{}{
		"name": "Go 1.2 is released",
		"url":  "golang.org"})
	if err != nil {
		panic(err)
	}

	// Read document
	readBack, err := feeds.Read(docID)
	if err != nil {
		panic(err)
	}
	fmt.Println("Document", docID, "is", readBack)

	// Update document
	err = feeds.Update(docID, map[string]interface{}{
		"name": "Go is very popular",
		"url":  "google.com"})
	if err != nil {
		panic(err)
	}

	// Process all documents (note that document order is undetermined)
	feeds.ForEachDoc(func(id int, docContent []byte) (willMoveOn bool) {
		fmt.Println("Document", id, "is", string(docContent))
		return true  // move on to the next document OR
		return false // do not move on to the next document
	})

	// Delete document
	if err := feeds.Delete(docID); err != nil {
		panic(err)
	}

	// More complicated error handing - identify the error Type.
	// In this example, the error code tells that the document no longer exists.
	if err := feeds.Delete(docID); dberr.Type(err) == dberr.ErrorNoDoc {
		fmt.Println("The document was already deleted")
	}

	// ****************** Index Management ******************
	// Indexes assist in many types of queries
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

	// Query result are document IDs
	for id := range queryResult {
		// To get query result document, simply read it
		readBack, err := feeds.Read(id)
		if err != nil {
			panic(err)
		}
		fmt.Printf("Query returned document %v\n", readBack)
	}

	// Gracefully close database
	if err := myDB.Close(); err != nil {
		panic(err)
	}
}
