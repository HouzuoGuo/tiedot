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
	var jsonDoc interface{}
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

func InsertWithUID(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "must-revalidate")
	w.Header().Set("Content-Type", "application/json")
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
	var jsonDoc interface{}
	if err := json.Unmarshal([]byte(doc), &jsonDoc); err != nil {
		http.Error(w, fmt.Sprintf("'%v' is not valid JSON document.", doc), 400)
		return
	}
	id, uid, err := dbcol.InsertWithUID(jsonDoc)
	if err != nil {
		http.Error(w, fmt.Sprint(err), 500)
		return
	}
	resp, err := json.Marshal(map[string]interface{}{"id": id, "uid": uid})
	if err != nil {
		http.Error(w, fmt.Sprint(err), 500)
		return
	}
	w.WriteHeader(201)
	w.Write(resp)
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
	err = dbcol.Read(uint64(docID), &doc)
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

func GetByUID(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "must-revalidate")
	w.Header().Set("Content-Type", "application/json")
	var col, uid string
	if !Require(w, r, "col", &col) {
		return
	}
	if !Require(w, r, "uid", &uid) {
		return
	}
	V3Sync.RLock()
	defer V3Sync.RUnlock()
	dbcol := V3DB.Use(col)
	if dbcol == nil {
		http.Error(w, fmt.Sprintf("Collection '%s' does not exist.", col), 400)
		return
	}
	var doc interface{}
	id, err := dbcol.ReadByUID(uid, &doc)
	if doc == nil {
		http.Error(w, fmt.Sprintf("No such document %s.", uid), 404)
		return
	}
	resp, err := json.Marshal(map[string]interface{}{"id": id, "doc": doc})
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
	var newDoc interface{}
	if err := json.Unmarshal([]byte(doc), &newDoc); err != nil {
		http.Error(w, fmt.Sprintf("'%v' is not valid JSON document.", newDoc), 400)
		return
	}
	newID, err := dbcol.Update(uint64(docID), newDoc)
	if err != nil {
		http.Error(w, fmt.Sprint(err), 500)
		return
	}
	w.Write([]byte(fmt.Sprint(newID)))
}

func UpdateByUID(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "must-revalidate")
	w.Header().Set("Content-Type", "text/json")
	var col, uid, doc string
	if !Require(w, r, "col", &col) {
		return
	}
	if !Require(w, r, "uid", &uid) {
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
	var newDoc interface{}
	if err := json.Unmarshal([]byte(doc), &newDoc); err != nil {
		http.Error(w, fmt.Sprintf("'%v' is not valid JSON document.", newDoc), 400)
		return
	}
	newID, err := dbcol.UpdateByUID(uid, newDoc)
	if err != nil {
		http.Error(w, fmt.Sprint(err), 500)
		return
	}
	w.Write([]byte(fmt.Sprint(newID)))
}

func ReassignUID(w http.ResponseWriter, r *http.Request) {
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
	newID, newUID, err := dbcol.ReassignUID(uint64(docID))
	if err != nil {
		http.Error(w, fmt.Sprint(err), 500)
	}
	resp, err := json.Marshal(map[string]interface{}{"id": newID, "uid": newUID})
	if err != nil {
		http.Error(w, fmt.Sprint(err), 500)
		return
	}
	w.WriteHeader(201)
	w.Write(resp)
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
	dbcol.Delete(uint64(docID))
}

func DeleteByUID(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "must-revalidate")
	w.Header().Set("Content-Type", "text/plain")
	var col, uid string
	if !Require(w, r, "col", &col) {
		return
	}
	if !Require(w, r, "uid", &uid) {
		return
	}
	V3Sync.RLock()
	defer V3Sync.RUnlock()
	dbcol := V3DB.Use(col)
	if dbcol == nil {
		http.Error(w, fmt.Sprintf("Collection '%s' does not exist.", col), 400)
		return
	}
	dbcol.DeleteByUID(uid)
}
