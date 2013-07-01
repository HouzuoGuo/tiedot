package main

import (
	"db"
	"encoding/json"
	"fmt"
	"os"
)

func embeddedExample() {
	dir := "/tmp/MyDatabase"
	os.RemoveAll(dir)
	defer os.RemoveAll(dir)

	// Open database
	myDB, err := db.OpenDB(dir)
	if err != nil {
		panic(err)
	}

	// Create collection
	if err := myDB.Create("A"); err != nil {
		panic(err)
	}
	if err := myDB.Create("B"); err != nil {
		panic(err)
	}

	// Rename collection
	if err := myDB.Rename("B", "C"); err != nil {
		panic(err)
	}

	// Which collections do I have?
	for name := range myDB.StrCol {
		fmt.Printf("I have a collection called %s\n", name)
	}

	// Drop collection
	if err := myDB.Drop("C"); err != nil {
		panic(err)
	}

	// Start using collection
	A := myDB.Use("A")

	// Insert document
	var doc interface{}
	json.Unmarshal([]byte(`{"a": 1, "b": 2}`), &doc)
	docID, err := A.Insert(doc)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Inserted document %v at %d (document ID)\n", doc, docID)

	// Update document
	json.Unmarshal([]byte(`{"a": 2, "b": 3}`), &doc)
	newID, err := A.Update(docID, doc) // newID may or may not be the same
	if err != nil {
		panic(err)
	}
	fmt.Printf("Updated document %d to %v, new ID is %d\n", docID, doc, newID)

	// Delete document
	A.Delete(123) // passing invalid ID to it will not harm your data

	// Create index
	if err := A.Index([]string{"a", "b", "c"}); err != nil {
		panic(err)
	}

	// Which indexes do I have on collection A?
	for path := range A.StrHT {
		fmt.Printf("I have an index on path %s\n", path)
	}

	// Remove index
	if err := A.Unindex([]string{"a", "b", "c"}); err != nil {
		panic(err)
	}

	// Execute query
	result := make(map[uint64]bool)
	var query interface{}
	json.Unmarshal([]byte(`["all"]`), &query)
	if err := db.EvalQuery(query, A, &result); err != nil {
		panic(err)
	}
	for id := range result {
		// query results are in map keys
		fmt.Printf("Query returned document ID %d\n", id)
	}

	// Gracefully close database
	myDB.Close()
}
