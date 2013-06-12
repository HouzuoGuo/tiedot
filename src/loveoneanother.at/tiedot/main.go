package main

import (
	"encoding/json"
	"fmt"
	"loveoneanother.at/tiedot/db"
)

func main() {
	col, err := db.OpenCol("/tmp/col")
	if err != nil {
		fmt.Println(err)
		return
	}

	docs := []string{`{"a": 1}`, `{"b": 2}`}
	var jsonDoc [2]interface{}
	json.Unmarshal([]byte(docs[0]), &jsonDoc[0])
	json.Unmarshal([]byte(docs[1]), &jsonDoc[1])

	updatedDocs := []string{`{"a": 2}`, `{"b": "abcdefghijklmnopqrstuvwxyz"}`}
	var updatedJsonDoc [2]interface{}
	json.Unmarshal([]byte(updatedDocs[0]), &updatedJsonDoc[0])
	json.Unmarshal([]byte(updatedDocs[1]), &updatedJsonDoc[1])

	ids := [2]uint64{}
	if ids[0], err = col.Insert(jsonDoc[0]); err != nil {
		fmt.Printf("Failed to insert: %v", err)
	}
	if ids[1], err = col.Insert(jsonDoc[1]); err != nil {
		fmt.Printf("Failed to insert: %v", err)
	}

	if ids[0], err = col.Update(ids[0], updatedJsonDoc[0]); err != nil {
		fmt.Printf("Failed to update: %v", err)
	}
	if ids[1], err = col.Update(ids[1], updatedJsonDoc[1]); err != nil {
		fmt.Printf("Failed to update: %v", err)
	}

	if col.Read(ids[0]).(map[string]interface{})[string('a')].(float64) != 2.0 {
		fmt.Printf("Failed to read back doc 0, %v", col.Read(ids[0]))
	}
	if col.Read(ids[1]).(map[string]interface{})[string('b')].(string) != string("abcdefghijklmnopqrstuvwxyz") {
		fmt.Printf("Failed to read back doc 1, %v", col.Read(ids[1]))
	}
}
