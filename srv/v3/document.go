/* Document management handlers. */
package v3

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
)

func Insert(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "must-revalidate")
	w.Header().Set("Content-Type", "text/plain")
	var col, doc string
	if !Require(w, r, "col", &col) {
		return
	}
	if !Require(w, r, "doc", &doc) {
		return
	}
	V3Sync.RLock()
	defer V3Sync.RUnlock()
	dbcol := V3DB.Use(col)
	if dbcol == nil {
		http.Error(w, fmt.Sprintf("Collection '%s' does not exist.", col), 400)
		return
	}
	var jsonDoc map[string]interface{}
	if err := json.Unmarshal([]byte(doc), &jsonDoc); err != nil {
		http.Error(w, fmt.Sprintf("'%v' is not valid JSON document.", doc), 400)
		return
	}
	id, err := dbcol.Insert(jsonDoc)
	if err != nil {
		http.Error(w, fmt.Sprint(err), 500)
		return
	}
	w.WriteHeader(201)
	w.Write([]byte(fmt.Sprint(id)))
}

func Get(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "must-revalidate")
	w.Header().Set("Content-Type", "application/json")
	var col, id string
	if !Require(w, r, "col", &col) {
		return
	}
	if !Require(w, r, "id", &id) {
		return
	}
	V3Sync.RLock()
	defer V3Sync.RUnlock()
	dbcol := V3DB.Use(col)
	if dbcol == nil {
		http.Error(w, fmt.Sprintf("Collection '%s' does not exist.", col), 400)
		return
	}
	docID, err := strconv.Atoi(id)
	if err != nil {
		http.Error(w, fmt.Sprintf("Invalid document ID '%v'.", id), 400)
		return
	}
	var doc interface{}
	_, err = dbcol.Read(docID, &doc)
	if doc == nil {
		http.Error(w, fmt.Sprintf("No such document ID %d.", docID), 404)
		return
	}
	resp, err := json.Marshal(doc)
	if err != nil {
		http.Error(w, fmt.Sprint(err), 500)
		return
	}
	w.Write(resp)
}

func Update(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "must-revalidate")
	w.Header().Set("Content-Type", "text/plain")
	var col, id, doc string
	if !Require(w, r, "col", &col) {
		return
	}
	if !Require(w, r, "id", &id) {
		return
	}
	if !Require(w, r, "doc", &doc) {
		return
	}
	V3Sync.RLock()
	defer V3Sync.RUnlock()
	dbcol := V3DB.Use(col)
	if dbcol == nil {
		http.Error(w, fmt.Sprintf("Collection '%s' does not exist.", col), 400)
		return
	}
	docID, err := strconv.Atoi(id)
	if err != nil {
		http.Error(w, fmt.Sprintf("Invalid document ID '%v'.", id), 400)
		return
	}
	var newDoc map[string]interface{}
	if err := json.Unmarshal([]byte(doc), &newDoc); err != nil {
		http.Error(w, fmt.Sprintf("'%v' is not valid JSON document.", newDoc), 400)
		return
	}
	err = dbcol.Update(docID, newDoc)
	if err != nil {
		http.Error(w, fmt.Sprint(err), 500)
		return
	}
}

func Delete(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "must-revalidate")
	w.Header().Set("Content-Type", "text/plain")
	var col, id string
	if !Require(w, r, "col", &col) {
		return
	}
	if !Require(w, r, "id", &id) {
		return
	}
	V3Sync.RLock()
	defer V3Sync.RUnlock()
	dbcol := V3DB.Use(col)
	if dbcol == nil {
		http.Error(w, fmt.Sprintf("Collection '%s' does not exist.", col), 400)
		return
	}
	docID, err := strconv.Atoi(id)
	if err != nil {
		http.Error(w, fmt.Sprintf("Invalid document ID '%v'.", id), 400)
		return
	}
	dbcol.Delete(docID)
}
