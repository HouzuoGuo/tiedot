// Query handlers.

package httpapi

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"

	"github.com/HouzuoGuo/tiedot/db"
)

// Execute a query and return documents from the result.
func Query(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "must-revalidate")
	w.Header().Set("Content-Type", "application/json")
	var col, q string
	if !Require(w, r, "col", &col) {
		return
	}
	if !Require(w, r, "q", &q) {
		return
	}
	var qJson interface{}
	if err := json.Unmarshal([]byte(q), &qJson); err != nil {
		http.Error(w, fmt.Sprintf("'%v' is not valid JSON.", q), 400)
		return
	}
	dbcol := HttpDB.Use(col)
	if dbcol == nil {
		http.Error(w, fmt.Sprintf("Collection '%s' does not exist.", col), 400)
		return
	}
	// Evaluate the query
	queryResult := make(map[int]struct{})
	if err := db.EvalQuery(qJson, dbcol, &queryResult); err != nil {
		http.Error(w, fmt.Sprint(err), 400)
		return
	}
	// Construct array of result
	resultDocs := make(map[string]interface{}, len(queryResult))
	counter := 0
	for docID := range queryResult {
		doc, _ := dbcol.Read(docID)
		if doc != nil {
			resultDocs[strconv.Itoa(docID)] = doc
			counter++
		}
	}
	// Serialize the array
	resp, err := json.Marshal(resultDocs)
	if err != nil {
		http.Error(w, fmt.Sprintf("Server error: query returned invalid structure"), 500)
		return
	}
	w.Write([]byte(string(resp)))
}

// Execute a query and return number of documents from the result.
func Count(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "must-revalidate")
	w.Header().Set("Content-Type", "text/plain")
	var col, q string
	if !Require(w, r, "col", &col) {
		return
	}
	if !Require(w, r, "q", &q) {
		return
	}
	var qJson interface{}
	if err := json.Unmarshal([]byte(q), &qJson); err != nil {
		http.Error(w, fmt.Sprintf("'%v' is not valid JSON.", q), 400)
		return
	}
	dbcol := HttpDB.Use(col)
	if dbcol == nil {
		http.Error(w, fmt.Sprintf("Collection '%s' does not exist.", col), 400)
		return
	}
	queryResult := make(map[int]struct{})
	if err := db.EvalQuery(qJson, dbcol, &queryResult); err != nil {
		http.Error(w, fmt.Sprint(err), 400)
		return
	}
	w.Write([]byte(strconv.Itoa(len(queryResult))))
}
