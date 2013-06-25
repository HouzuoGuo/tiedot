/* Collection management handlers. */
package v1

import (
	"encoding/json"
	"fmt"
	"net/http"
)

func Create(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "must-revalidate")
	var name string
	if !Require(w, r, "name", &name) {
		return
	}
	V1Sync.Lock()
	defer V1Sync.Unlock()
	if err := V1DB.Create(name); err != nil {
		http.Error(w, fmt.Sprint(err), 400)
	} else {
		w.WriteHeader(201)
	}
}

func All(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "must-revalidate")
	cols := make([]string, 0)
	V1Sync.Lock()
	defer V1Sync.Unlock()
	for k := range V1DB.StrCol {
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
	var oldName, newName string
	if !Require(w, r, "old", &oldName) {
		return
	}
	if !Require(w, r, "new", &newName) {
		return
	}
	V1Sync.Lock()
	defer V1Sync.Unlock()
	if err := V1DB.Rename(oldName, newName); err != nil {
		http.Error(w, fmt.Sprint(err), 400)
	}
}

func Drop(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "must-revalidate")
	var name string
	if !Require(w, r, "name", &name) {
		return
	}
	V1Sync.Lock()
	defer V1Sync.Unlock()
	if err := V1DB.Drop(name); err != nil {
		http.Error(w, fmt.Sprint(err), 400)
	}
}

func Scrub(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "must-revalidate")
	var name string
	if !Require(w, r, "name", &name) {
		return
	}
	V1Sync.Lock()
	defer V1Sync.Unlock()
	if err := V1DB.Scrub(name); err != nil {
		http.Error(w, fmt.Sprint(err), 400)
	}
}
