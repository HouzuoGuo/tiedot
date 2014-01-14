package main

import (
	"encoding/json"
	"fmt"
	"github.com/HouzuoGuo/tiedot/db"
	"os"
)

// You are encouraged to use (nearly) all tiedot public functions concurrently.
// There are few exceptions - see individual package/functions for details.

func embeddedExample() {
	// ****************** Collection Management ******************
	// Create and open database
	dir := "/tmp/MyDatabase"
	os.RemoveAll(dir)
	defer os.RemoveAll(dir)

	myDB, err := db.OpenDB(dir)
	if err != nil {
		panic(err)
	}

	// Create two collections Feeds and Votes
	// "2" means collection data and indexes are divided into two halves, allowing concurrent access from two threads
	if err := myDB.Create("Feeds", 2); err != nil {
		panic(err)
	}
	if err := myDB.Create("Votes", 2); err != nil {
		panic(err)
	}

	// What collections do I now have?
	for name := range myDB.StrCol {
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
	feeds := myDB.Use("Feeds")

	// Insert document (document must be map[string]interface{})
	docID, err := feeds.Insert(map[string]interface{}{
		"name": "Go 1.2 is released",
		"url":  "golang.org"})
	if err != nil {
		panic(err)
	}

	// Read document
	var readBack interface{}
	feeds.Read(docID, &readBack) // pass in document's physical ID
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

	// Delete document
	feeds.Delete(123) // An ID which does not exist does no harm

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
	for path := range feeds.SecIndexes {
		fmt.Printf("I have an index on path %s\n", path)
	}

	// Remove index
	if err := feeds.Unindex([]string{"author", "name", "first_name"}); err != nil {
		panic(err)
	}

	// ****************** Queries ******************
	// Let's prepare a number of docments for a start
	feeds.Insert(map[string]interface{}{"Title": "New Go release", "Source": "golang.org", "Age": 3})
	feeds.Insert(map[string]interface{}{"Title": "Kitkat is here", "Source": "google.com", "Age": 2})
	feeds.Insert(map[string]interface{}{"Title": "Good Slackware", "Source": "slackware.com", "Age": 1})

	queryStr := `[{"eq": "New Go release", "in": ["Title"]}, {"eq": "slackware.com", "in": ["Source"]}]`
	var query interface{}
	json.Unmarshal([]byte(queryStr), &query)

	queryResult := make(map[int]struct{}) // query result (document IDs) goes into map keys

	if err := db.EvalQuery(query, feeds, &queryResult); err != nil {
		panic(err)
	}

	// Query results are physical document IDs
	for id := range queryResult {
		fmt.Printf("Query returned document ID %d\n", id)
	}

	// To use the document itself, simply read it back
	for id := range queryResult {
		feeds.Read(id, &readBack)
		fmt.Printf("Query returned document %v\n", readBack)
	}

	// Gracefully close database
	myDB.Close()
}
