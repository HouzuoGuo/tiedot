// Document management handlers.

package httpapi

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
)

// Insert a document into collection.
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
	var jsonDoc map[string]interface{}
	if err := json.Unmarshal([]byte(doc), &jsonDoc); err != nil {
		http.Error(w, fmt.Sprintf("'%v' is not valid JSON document.", doc), 400)
		return
	}
	dbcol := HttpDB.Use(col)
	if dbcol == nil {
		http.Error(w, fmt.Sprintf("Collection '%s' does not exist.", col), 400)
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

// Find and retrieve a document by ID.
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
	docID, err := strconv.Atoi(id)
	if err != nil {
		http.Error(w, fmt.Sprintf("Invalid document ID '%v'.", id), 400)
		return
	}
	dbcol := HttpDB.Use(col)
	if dbcol == nil {
		http.Error(w, fmt.Sprintf("Collection '%s' does not exist.", col), 400)
		return
	}
	doc, err := dbcol.Read(docID)
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

// Divide documents into roughly equally sized pages, and return documents in the specified page.
func GetPage(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "must-revalidate")
	w.Header().Set("Content-Type", "application/json")
	var col, page, total string
	if !Require(w, r, "col", &col) {
		return
	}
	if !Require(w, r, "page", &page) {
		return
	}
	if !Require(w, r, "total", &total) {
		return
	}
	totalPage, err := strconv.Atoi(total)
	if err != nil || totalPage < 1 {
		http.Error(w, fmt.Sprintf("Invalid total page number '%v'.", totalPage), 400)
		return
	}
	pageNum, err := strconv.Atoi(page)
	if err != nil || pageNum < 0 || pageNum >= totalPage {
		http.Error(w, fmt.Sprintf("Invalid page number '%v'.", page), 400)
		return
	}
	dbcol := HttpDB.Use(col)
	if dbcol == nil {
		http.Error(w, fmt.Sprintf("Collection '%s' does not exist.", col), 400)
		return
	}
	docs := make(map[string]interface{})
	dbcol.ForEachDocInPage(pageNum, totalPage, func(id int, doc []byte) bool {
		var docObj map[string]interface{}
		if err := json.Unmarshal(doc, &docObj); err == nil {
			docs[strconv.Itoa(id)] = docObj
		}
		return true
	})
	resp, err := json.Marshal(docs)
	if err != nil {
		http.Error(w, fmt.Sprint(err), 500)
		return
	}
	w.Write(resp)
}

// Update a document.
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
	dbcol := HttpDB.Use(col)
	if dbcol == nil {
		http.Error(w, fmt.Sprintf("Collection '%s' does not exist.", col), 400)
		return
	}
	err = dbcol.Update(docID, newDoc)
	if err != nil {
		http.Error(w, fmt.Sprint(err), 500)
		return
	}
}

// Delete a document.
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
	docID, err := strconv.Atoi(id)
	if err != nil {
		http.Error(w, fmt.Sprintf("Invalid document ID '%v'.", id), 400)
		return
	}
	dbcol := HttpDB.Use(col)
	if dbcol == nil {
		http.Error(w, fmt.Sprintf("Collection '%s' does not exist.", col), 400)
		return
	}
	dbcol.Delete(docID)
}

// Return approximate number of documents in the collection.
func ApproxDocCount(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "must-revalidate")
	w.Header().Set("Content-Type", "text/plain")
	var col string
	if !Require(w, r, "col", &col) {
		return
	}
	dbcol := HttpDB.Use(col)
	if dbcol == nil {
		http.Error(w, fmt.Sprintf("Collection '%s' does not exist.", col), 400)
		return
	}
	w.Write([]byte(strconv.Itoa(dbcol.ApproxDocCount())))
}
