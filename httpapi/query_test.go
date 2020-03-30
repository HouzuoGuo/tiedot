package httpapi

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/HouzuoGuo/tiedot/db"
)

var (
	requestQuery        = "http://localhost:8080/query"
	requestQueryWithCol = "http://localhost:8080/query?col=%s"
	requestQueryWithAll = "http://localhost:8080/query?col=%s&q=%s"

	requestCount        = "http://localhost:8080/count"
	requestCountWithCol = "http://localhost:8080/count?col=%s"
	requestCountWithAll = "http://localhost:8080/count?col=%s&q=%s"
)

func TestQueryNotCol(t *testing.T) {
	req := httptest.NewRequest(RandMethodRequest(), requestQuery, nil)
	w := httptest.NewRecorder()
	Query(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("Expected status %d", http.StatusBadRequest)
	}
}
func TestQueryNotQ(t *testing.T) {
	req := httptest.NewRequest(RandMethodRequest(), fmt.Sprintf(requestQueryWithCol, "col"), nil)
	w := httptest.NewRecorder()
	Query(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("Expected status %d", http.StatusBadRequest)
	}
}
func TestQueryCollectionNot(t *testing.T) {
	setupTestCase()
	defer tearDownTestCase()
	badColl := "notExistCol"
	reqCreate := httptest.NewRequest(RandMethodRequest(), requestCreate, nil)
	req := httptest.NewRequest(RandMethodRequest(), fmt.Sprintf(requestQueryWithAll, badColl, "1"), nil)
	w := httptest.NewRecorder()
	wQuery := httptest.NewRecorder()
	var err error
	if HttpDB, err = db.OpenDB(tempDir); err != nil {
		panic(err)
	}
	Create(w, reqCreate)
	Query(wQuery, req)
	if wQuery.Code != http.StatusBadRequest || strings.TrimSpace(wQuery.Body.String()) != fmt.Sprintf("Collection '%s' does not exist.", badColl) {
		t.Errorf("Expected status %d and error message collection not exist", http.StatusBadRequest)
	}
}
func TestQueryJsonIsNotValid(t *testing.T) {
	setupTestCase()
	defer tearDownTestCase()
	badJson := "1asc"
	reqCreate := httptest.NewRequest(RandMethodRequest(), requestCreate, nil)
	req := httptest.NewRequest(RandMethodRequest(), fmt.Sprintf(requestQueryWithAll, collection, badJson), nil)
	w := httptest.NewRecorder()
	wQuery := httptest.NewRecorder()
	var err error
	if HttpDB, err = db.OpenDB(tempDir); err != nil {
		panic(err)
	}
	Create(w, reqCreate)
	Query(wQuery, req)
	if wQuery.Code != http.StatusBadRequest || strings.TrimSpace(wQuery.Body.String()) != fmt.Sprintf("'%s' is not valid JSON.", badJson) {
		t.Errorf("Expected status %d and error message json is not valid", http.StatusOK)
	}
}

func TestCountNotCol(t *testing.T) {
	req := httptest.NewRequest(RandMethodRequest(), requestCount, nil)
	w := httptest.NewRecorder()
	Count(w, req)

	if w.Code != http.StatusBadRequest || strings.TrimSpace(w.Body.String()) != "Please pass POST/PUT/GET parameter value of 'col'." {
		t.Errorf("Expected status %d and error message not parameter 'col' ", http.StatusBadRequest)
	}
}
func TestCountNotParameterQ(t *testing.T) {
	req := httptest.NewRequest(RandMethodRequest(), fmt.Sprintf(requestCountWithCol, collection), nil)
	w := httptest.NewRecorder()
	Count(w, req)

	if w.Code != http.StatusBadRequest || strings.TrimSpace(w.Body.String()) != "Please pass POST/PUT/GET parameter value of 'q'." {
		t.Errorf("Expected status %d and error message not parameter 'col' ", http.StatusBadRequest)
	}
}
func TestCountCollNotExist(t *testing.T) {
	setupTestCase()
	defer tearDownTestCase()
	req := httptest.NewRequest(RandMethodRequest(), fmt.Sprintf(requestCountWithAll, collection, "1"), nil)
	w := httptest.NewRecorder()
	var err error
	if HttpDB, err = db.OpenDB(tempDir); err != nil {
		panic(err)
	}
	Count(w, req)

	if w.Code != http.StatusBadRequest || strings.TrimSpace(w.Body.String()) != fmt.Sprintf("Collection '%s' does not exist.", collection) {
		t.Errorf("Expected status %d and error message collection not exist", http.StatusBadRequest)
	}
}
func TestCount(t *testing.T) {
	setupTestCase()
	defer tearDownTestCase()
	reqCreate := httptest.NewRequest(RandMethodRequest(), requestCreate, nil)
	req := httptest.NewRequest(RandMethodRequest(), fmt.Sprintf(requestCountWithAll, collection, "1"), nil)
	w := httptest.NewRecorder()
	wCount := httptest.NewRecorder()

	var err error
	if HttpDB, err = db.OpenDB(tempDir); err != nil {
		panic(err)
	}
	Create(w, reqCreate)
	Count(wCount, req)
	if wCount.Code != http.StatusOK || wCount.Body.String() != "0" {
		t.Errorf("Expected status %d and count '0' ", http.StatusOK)
	}
}
func TestCountJsonIsNotValid(t *testing.T) {
	setupTestCase()
	defer tearDownTestCase()
	badJson := "1asc"
	reqCreate := httptest.NewRequest(RandMethodRequest(), requestCreate, nil)
	req := httptest.NewRequest(RandMethodRequest(), fmt.Sprintf(requestCountWithAll, collection, badJson), nil)
	w := httptest.NewRecorder()
	wCount := httptest.NewRecorder()

	var err error
	if HttpDB, err = db.OpenDB(tempDir); err != nil {
		panic(err)
	}
	Create(w, reqCreate)
	Count(wCount, req)

	if wCount.Code != http.StatusBadRequest || strings.TrimSpace(wCount.Body.String()) != fmt.Sprintf("'%s' is not valid JSON.", badJson) {
		t.Errorf("Expected status %d and json is not valid ", http.StatusBadRequest)
	}
}
