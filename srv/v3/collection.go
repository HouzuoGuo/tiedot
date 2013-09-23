/* Collection management handlers. */
package v2

import (
	"encoding/json"
	"fmt"
	"net/http"
)

func Create(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "must-revalidate")
	w.Header().Set("Content-Type", "application/json")
	var col string
	if !Require(w, r, "col", &col) {
		return
	}
	V2Sync.Lock()
	defer V2Sync.Unlock()
	if err := V2DB.Create(col); err != nil {
		http.Error(w, fmt.Sprint(err), 400)
	} else {
		w.WriteHeader(201)
	}
}

func All(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "must-revalidate")
	w.Header().Set("Content-Type", "application/json")
	cols := make([]string, 0)
	V2Sync.Lock()
	defer V2Sync.Unlock()
	for k := range V2DB.StrCol {
		cols = append(cols, k)
	}
	resp, err := json.Marshal(cols)
	if err != nil {
		http.Error(w, fmt.Sprint(err), 500)
		return
	}
	w.Write(resp)
}

func Rename(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "must-revalidate")
	w.Header().Set("Content-Type", "application/json")
	var oldName, newName string
	if !Require(w, r, "old", &oldName) {
		return
	}
	if !Require(w, r, "new", &newName) {
		return
	}
	V2Sync.Lock()
	defer V2Sync.Unlock()
	if err := V2DB.Rename(oldName, newName); err != nil {
		http.Error(w, fmt.Sprint(err), 400)
	}
}

func Drop(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "must-revalidate")
	w.Header().Set("Content-Type", "application/json")
	var col string
	if !Require(w, r, "col", &col) {
		return
	}
	V2Sync.Lock()
	defer V2Sync.Unlock()
	if err := V2DB.Drop(col); err != nil {
		http.Error(w, fmt.Sprint(err), 400)
	}
}

func Scrub(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "must-revalidate")
	w.Header().Set("Content-Type", "application/json")
	var col string
	if !Require(w, r, "col", &col) {
		return
	}
	V2Sync.Lock()
	defer V2Sync.Unlock()
	if err := V2DB.Scrub(col); err != nil {
		http.Error(w, fmt.Sprint(err), 400)
	}
}
