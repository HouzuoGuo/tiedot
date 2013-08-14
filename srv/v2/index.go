/* Index management handlers. */
package v2

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
)

func Index(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "must-revalidate")
	w.Header().Set("Content-Type", "application/json")
	var col, path string
	if !Require(w, r, "col", &col) {
		return
	}
	if !Require(w, r, "path", &path) {
		return
	}
	dbcol := V2DB.Use(col)
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

func Indexes(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "must-revalidate")
	w.Header().Set("Content-Type", "application/json")
	var col string
	if !Require(w, r, "col", &col) {
		return
	}
	dbcol := V2DB.Use(col)
	if dbcol == nil {
		http.Error(w, fmt.Sprintf("Collection '%s' does not exist.", col), 400)
		return
	}
	indexes := make([]string, 0)
	for path := range dbcol.StrHT {
		indexes = append(indexes, path)
	}
	resp, err := json.Marshal(indexes)
	if err != nil {
		http.Error(w, fmt.Sprint("Server error."), 500)
		return
	}
	w.Write(resp)
}

func Unindex(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "must-revalidate")
	w.Header().Set("Content-Type", "application/json")
	var col, path string
	if !Require(w, r, "col", &col) {
		return
	}
	if !Require(w, r, "path", &path) {
		return
	}
	dbcol := V2DB.Use(col)
	if dbcol == nil {
		http.Error(w, fmt.Sprintf("Collection '%s' does not exist.", col), 400)
		return
	}
	if err := dbcol.Unindex(strings.Split(path, ",")); err != nil {
		http.Error(w, fmt.Sprint(err), 400)
		return
	}
}
