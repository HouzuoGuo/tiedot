/* Query handlers. */
package v1

import (
	"db"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
)

func Query(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "must-revalidate")
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
	V1Sync.RLock()
	defer V1Sync.RUnlock()
	dbcol := V1DB.Use(col)
	if dbcol == nil {
		http.Error(w, fmt.Sprintf("Collection '%s' does not exist.", col), 400)
		return
	}
	// evaluate the query
	queryResult := make(map[uint64]bool)
	if err := db.EvalQuery(qJson, dbcol, &queryResult); err != nil {
		http.Error(w, fmt.Sprint(err), 400)
		return
	}
	// write each document on a new line
	for k := range queryResult {
		doc, _ := dbcol.Read(k)
		if doc == nil {
			continue
		}
		resp, err := json.Marshal(doc)
		if err != nil {
			log.Printf("Query returned invalid JSON '%v'", doc)
			continue
		}
		w.Write([]byte(string(resp) + "\n"))
	}
}

func QueryID(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "must-revalidate")
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
	V1Sync.RLock()
	defer V1Sync.RUnlock()
	dbcol := V1DB.Use(col)
	if dbcol == nil {
		http.Error(w, fmt.Sprintf("Collection '%s' does not exist.", col), 400)
		return
	}
	queryResult := make(map[uint64]bool)
	if err := db.EvalQuery(qJson, dbcol, &queryResult); err != nil {
		http.Error(w, fmt.Sprint(err), 400)
		return
	}
	for k := range queryResult {
		w.Write([]byte(fmt.Sprintf("%d\n", k)))
	}
}

func Count(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "must-revalidate")
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
	V1Sync.RLock()
	defer V1Sync.RUnlock()
	dbcol := V1DB.Use(col)
	if dbcol == nil {
		http.Error(w, fmt.Sprintf("Collection '%s' does not exist.", col), 400)
		return
	}
	queryResult := make(map[uint64]bool)
	if err := db.EvalQuery(qJson, dbcol, &queryResult); err != nil {
		http.Error(w, fmt.Sprint(err), 400)
		return
	}
	w.Write([]byte(strconv.Itoa(len(queryResult))))
}
