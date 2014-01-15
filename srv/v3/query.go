/* Query handlers. */
package v3

import (
	"encoding/json"
	"fmt"
	"github.com/HouzuoGuo/tiedot/db"
	"net/http"
	"strconv"
)

func Query(w http.ResponseWriter, r *http.Request) {
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
	V3Sync.RLock()
	defer V3Sync.RUnlock()
	dbcol := V3DB.Use(col)
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
	resultDocs := make([]map[string]interface{}, len(queryResult))
	counter := 0
	for docID := range queryResult {
		var doc map[string]interface{}
		dbcol.Read(docID, &doc)
		if doc != nil {
			resultDocs[counter] = doc
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
	V3Sync.RLock()
	defer V3Sync.RUnlock()
	dbcol := V3DB.Use(col)
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
