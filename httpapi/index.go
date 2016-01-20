// Index management handlers.

package httpapi

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
)

// Put an index on a document path.
func Index(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "must-revalidate")
	w.Header().Set("Content-Type", "text/plain")
	var col, path string
	if !Require(w, r, "col", &col) {
		return
	}
	if !Require(w, r, "path", &path) {
		return
	}
	dbcol := HttpDB.Use(col)
	if dbcol == nil {
		http.Error(w, fmt.Sprintf("Collection '%s' does not exist.", col), 400)
		return
	}
	if err := dbcol.Index(strings.Split(path, ",")); err != nil {
		http.Error(w, fmt.Sprint(err), 400)
		return
	}
	w.WriteHeader(201)
}

// Return all indexed paths.
func Indexes(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "must-revalidate")
	w.Header().Set("Content-Type", "application/json")
	var col string
	if !Require(w, r, "col", &col) {
		return
	}
	dbcol := HttpDB.Use(col)
	if dbcol == nil {
		http.Error(w, fmt.Sprintf("Collection '%s' does not exist.", col), 400)
		return
	}
	indexes := make([][]string, 0)
	for _, path := range dbcol.AllIndexes() {
		indexes = append(indexes, path)
	}
	resp, err := json.Marshal(indexes)
	if err != nil {
		http.Error(w, fmt.Sprint("Server error."), 500)
		return
	}
	w.Write(resp)
}

// Remove an indexed path.
func Unindex(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "must-revalidate")
	w.Header().Set("Content-Type", "text/plain")
	var col, path string
	if !Require(w, r, "col", &col) {
		return
	}
	if !Require(w, r, "path", &path) {
		return
	}
	dbcol := HttpDB.Use(col)
	if dbcol == nil {
		http.Error(w, fmt.Sprintf("Collection '%s' does not exist.", col), 400)
		return
	}
	if err := dbcol.Unindex(strings.Split(path, ",")); err != nil {
		http.Error(w, fmt.Sprint(err), 400)
		return
	}
}
