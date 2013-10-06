/* Collection management handlers. */
package v3

import (
	"encoding/json"
	"fmt"
	"net/http"
)

func Create(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "must-revalidate")
	w.Header().Set("Content-Type", "text/plain")
	var col string
	if !Require(w, r, "col", &col) {
		return
	}
	V3Sync.Lock()
	defer V3Sync.Unlock()
	if err := V3DB.Create(col); err != nil {
		http.Error(w, fmt.Sprint(err), 400)
	} else {
		w.WriteHeader(201)
	}
}

func All(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "must-revalidate")
	w.Header().Set("Content-Type", "application/json")
	cols := make([]string, 0)
	V3Sync.Lock()
	defer V3Sync.Unlock()
	for k := range V3DB.StrCol {
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
	w.Header().Set("Content-Type", "text/plain")
	var oldName, newName string
	if !Require(w, r, "old", &oldName) {
		return
	}
	if !Require(w, r, "new", &newName) {
		return
	}
	V3Sync.Lock()
	defer V3Sync.Unlock()
	if err := V3DB.Rename(oldName, newName); err != nil {
		http.Error(w, fmt.Sprint(err), 400)
	}
}

func Drop(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "must-revalidate")
	w.Header().Set("Content-Type", "text/plain")
	var col string
	if !Require(w, r, "col", &col) {
		return
	}
	V3Sync.Lock()
	defer V3Sync.Unlock()
	if err := V3DB.Drop(col); err != nil {
		http.Error(w, fmt.Sprint(err), 400)
	}
}

func Scrub(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "must-revalidate")
	w.Header().Set("Content-Type", "text/plain")
	var col string
	if !Require(w, r, "col", &col) {
		return
	}
	V3Sync.Lock()
	defer V3Sync.Unlock()
	if err := V3DB.Scrub(col); err != nil {
		http.Error(w, fmt.Sprint(err), 400)
	}
}
