/* Collection management handlers. */
package httpapi

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
	HttpDBSync.Lock()
	defer HttpDBSync.Unlock()
	if err := HttpDB.Create(col); err != nil {
		http.Error(w, fmt.Sprint(err), 400)
	} else {
		w.WriteHeader(201)
	}
}

func All(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "must-revalidate")
	w.Header().Set("Content-Type", "application/json")
	cols := make([]string, 0)
	HttpDBSync.Lock()
	defer HttpDBSync.Unlock()
	for _, v := range HttpDB.AllCols() {
		cols = append(cols, v)
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
	HttpDBSync.Lock()
	defer HttpDBSync.Unlock()
	if err := HttpDB.Rename(oldName, newName); err != nil {
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
	HttpDBSync.Lock()
	defer HttpDBSync.Unlock()
	if err := HttpDB.Drop(col); err != nil {
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
	HttpDBSync.Lock()
	defer HttpDBSync.Unlock()
	dbCol := HttpDB.Use(col)
	if dbCol == nil {
		http.Error(w, fmt.Sprintf("Collection %s does not exist", col), 400)
	} else {
		HttpDB.Scrub(col)
	}
}

func Sync(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "must-revalidate")
	w.Header().Set("Content-Type", "text/plain")
	HttpDBSync.Lock()
	defer HttpDBSync.Unlock()
	HttpDB.Sync()
}
