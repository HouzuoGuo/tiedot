package httpapi

import (
	"bytes"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/HouzuoGuo/tiedot/db"
)

var (
	requestIndex     = "http://localhost:8080/index?col=%s&path=%s"
	requestIndexes   = "http://localhost:8080/indexes?col=%s"
	requestUnIndexes = "http://localhost:8080/unindex?col=%s&path=%s"

	path = "a"
)

func TestIndex(t *testing.T) {
	testsIndex := []func(t *testing.T){
		TIndex,
		TIndexNotCol,
		TIndexNotPath,
		TIndexError,
		TIndexError,
		TIndexCollNotExist,
		TIndexesNotCol,
		TIndexesCollNotExist,
		TUnIndexes,
		TUnIndexesColNotExist,
		TUnIndexNotCol,
		TUnIndexNotPath,
		TUnIndexErrorNotHave,
	}
	managerSubTests(testsIndex, "index_test", t)
}

// Index
func TIndex(t *testing.T) {
	setupTestCase()
	defer tearDownTestCase()

	b := &bytes.Buffer{}
	b.WriteString("{\"a\": 1, \"b\": 2}")

	reqCreate := httptest.NewRequest("GET", requestCreate, nil)
	reqInsert := httptest.NewRequest(RandMethodRequest(), requestInsertWithoutDoc, b)
	reqIndex := httptest.NewRequest(RandMethodRequest(), fmt.Sprintf(requestIndex, collection, path), nil)
	reqIndexes := httptest.NewRequest(RandMethodRequest(), fmt.Sprintf(requestIndexes, collection), nil)

	wCreate := httptest.NewRecorder()
	wInsert := httptest.NewRecorder()
	wIndex := httptest.NewRecorder()
	wIndexes := httptest.NewRecorder()

	var err error
	if HttpDB, err = db.OpenDB(tempDir); err != nil {
		panic(err)
	}
	Create(wCreate, reqCreate)
	Insert(wInsert, reqInsert)
	Index(wIndex, reqIndex)
	Indexes(wIndexes, reqIndexes)

	if wIndex.Code != 201 || wIndexes.Code != 200 || wIndexes.Body.String() != "[[\"a\"]]" {
		t.Error("Expected code 201 and get list Indexes after insert")
	}
}
func TIndexNotCol(t *testing.T) {
	setupTestCase()
	defer tearDownTestCase()

	reqIndex := httptest.NewRequest(RandMethodRequest(), fmt.Sprintf(requestIndex, "", ""), nil)
	wIndex := httptest.NewRecorder()

	var err error
	if HttpDB, err = db.OpenDB(tempDir); err != nil {
		panic(err)
	}
	Index(wIndex, reqIndex)
	if wIndex.Code != 400 || strings.TrimSpace(wIndex.Body.String()) != "Please pass POST/PUT/GET parameter value of 'col'." {
		t.Error("Expected code 400 and get message error not parameter 'col'")
	}
}
func TIndexNotPath(t *testing.T) {
	setupTestCase()
	defer tearDownTestCase()

	reqIndex := httptest.NewRequest(RandMethodRequest(), fmt.Sprintf(requestIndex, collection, ""), nil)
	wIndex := httptest.NewRecorder()

	var err error
	if HttpDB, err = db.OpenDB(tempDir); err != nil {
		panic(err)
	}
	Index(wIndex, reqIndex)
	if wIndex.Code != 400 || strings.TrimSpace(wIndex.Body.String()) != "Please pass POST/PUT/GET parameter value of 'path'." {
		t.Error("Expected code 400 and get message error not parameter 'path'")
	}
}
func TIndexError(t *testing.T) {
	setupTestCase()
	defer tearDownTestCase()

	b := &bytes.Buffer{}
	b.WriteString("{\"a\": 1, \"b\": 2}")

	reqCreate := httptest.NewRequest("GET", requestCreate, nil)
	reqInsert := httptest.NewRequest(RandMethodRequest(), requestInsertWithoutDoc, b)
	reqIndex := httptest.NewRequest(RandMethodRequest(), fmt.Sprintf(requestIndex, collection, path), nil)
	reqIndexErr := httptest.NewRequest(RandMethodRequest(), fmt.Sprintf(requestIndex, collection, path), nil)

	wCreate := httptest.NewRecorder()
	wInsert := httptest.NewRecorder()
	wIndex := httptest.NewRecorder()
	wIndexErr := httptest.NewRecorder()

	var err error
	if HttpDB, err = db.OpenDB(tempDir); err != nil {
		panic(err)
	}
	Create(wCreate, reqCreate)
	Insert(wInsert, reqInsert)
	Index(wIndex, reqIndex)
	Index(wIndexErr, reqIndexErr)

	if wIndexErr.Code != 400 || strings.TrimSpace(wIndexErr.Body.String()) != "Path [a] is already indexed" {
		t.Error("Expected code 400 and message is already indexed.")
	}
}
func TIndexCollNotExist(t *testing.T) {
	setupTestCase()
	defer tearDownTestCase()

	b := &bytes.Buffer{}
	b.WriteString("{\"a\": 1, \"b\": 2}")

	reqIndex := httptest.NewRequest(RandMethodRequest(), fmt.Sprintf(requestIndex, collection, path), nil)
	wIndex := httptest.NewRecorder()

	var err error
	if HttpDB, err = db.OpenDB(tempDir); err != nil {
		panic(err)
	}
	Index(wIndex, reqIndex)

	if wIndex.Code != 400 || strings.TrimSpace(wIndex.Body.String()) != fmt.Sprintf("Collection '%s' does not exist.", collection) {
		t.Error("Expected code 400 and message collection does not exist.")
	}
}

// Indexes
func TIndexesNotCol(t *testing.T) {
	setupTestCase()
	defer tearDownTestCase()

	reqIndexes := httptest.NewRequest(RandMethodRequest(), fmt.Sprintf(requestIndexes, ""), nil)
	wIndexes := httptest.NewRecorder()

	var err error
	if HttpDB, err = db.OpenDB(tempDir); err != nil {
		panic(err)
	}
	Indexes(wIndexes, reqIndexes)

	if wIndexes.Code != 400 || strings.TrimSpace(wIndexes.Body.String()) != "Please pass POST/PUT/GET parameter value of 'col'." {
		t.Error("Expected code 400 and error message not parameter 'col'.")
	}
}
func TIndexesCollNotExist(t *testing.T) {
	setupTestCase()
	defer tearDownTestCase()

	reqIndexes := httptest.NewRequest(RandMethodRequest(), fmt.Sprintf(requestIndexes, collection), nil)
	wIndexes := httptest.NewRecorder()

	var err error
	if HttpDB, err = db.OpenDB(tempDir); err != nil {
		panic(err)
	}
	Indexes(wIndexes, reqIndexes)
	if wIndexes.Code != 400 || strings.TrimSpace(wIndexes.Body.String()) != fmt.Sprintf("Collection '%s' does not exist.", collection) {
		t.Error("Expected code 400 and error message 'collection does not exist' .")
	}
}

// UnIndexes
func TUnIndexes(t *testing.T) {
	setupTestCase()
	defer tearDownTestCase()

	b := &bytes.Buffer{}
	b.WriteString("{\"a\": 1, \"b\": 2}")

	reqCreate := httptest.NewRequest("GET", requestCreate, nil)
	reqInsert := httptest.NewRequest(RandMethodRequest(), requestInsertWithoutDoc, b)
	reqIndex := httptest.NewRequest(RandMethodRequest(), fmt.Sprintf(requestIndex, collection, path), nil)
	reqUnIndex := httptest.NewRequest(RandMethodRequest(), fmt.Sprintf(requestUnIndexes, collection, path), nil)
	reqIndexes := httptest.NewRequest(RandMethodRequest(), fmt.Sprintf(requestIndexes, collection), nil)

	wCreate := httptest.NewRecorder()
	wInsert := httptest.NewRecorder()
	wIndex := httptest.NewRecorder()
	wUnIndex := httptest.NewRecorder()
	wIndexes := httptest.NewRecorder()

	var err error
	if HttpDB, err = db.OpenDB(tempDir); err != nil {
		panic(err)
	}
	Create(wCreate, reqCreate)
	Insert(wInsert, reqInsert)
	Index(wIndex, reqIndex)
	Unindex(wUnIndex, reqUnIndex)
	Indexes(wIndexes, reqIndexes)

	if wUnIndex.Code != 200 || wIndexes.Code != 200 || wIndexes.Body.String() != "[]" {
		t.Error("Expected code 200 and get empty message []")
	}
}
func TUnIndexesColNotExist(t *testing.T) {
	setupTestCase()
	defer tearDownTestCase()

	reqUnIndexes := httptest.NewRequest(RandMethodRequest(), fmt.Sprintf(requestUnIndexes, collection, path), nil)
	wUnIndexes := httptest.NewRecorder()

	var err error
	if HttpDB, err = db.OpenDB(tempDir); err != nil {
		panic(err)
	}
	Unindex(wUnIndexes, reqUnIndexes)

	if wUnIndexes.Code != 400 || strings.TrimSpace(wUnIndexes.Body.String()) != fmt.Sprintf("Collection '%s' does not exist.", collection) {
		t.Error("Expected code 400 and error message 'collection does not exist' .")
	}
}
func TUnIndexNotCol(t *testing.T) {
	setupTestCase()
	defer tearDownTestCase()

	reqUnIndex := httptest.NewRequest(RandMethodRequest(), fmt.Sprintf(requestUnIndexes, "", ""), nil)
	wUnIndex := httptest.NewRecorder()

	var err error
	if HttpDB, err = db.OpenDB(tempDir); err != nil {
		panic(err)
	}
	Unindex(wUnIndex, reqUnIndex)
	if wUnIndex.Code != 400 || strings.TrimSpace(wUnIndex.Body.String()) != "Please pass POST/PUT/GET parameter value of 'col'." {
		t.Error("Expected code 400 and get message error not parameter 'col'")
	}
}
func TUnIndexNotPath(t *testing.T) {
	setupTestCase()
	defer tearDownTestCase()

	reqUnIndex := httptest.NewRequest(RandMethodRequest(), fmt.Sprintf(requestUnIndexes, collection, ""), nil)
	wUnIndex := httptest.NewRecorder()

	var err error
	if HttpDB, err = db.OpenDB(tempDir); err != nil {
		panic(err)
	}
	Unindex(wUnIndex, reqUnIndex)
	if wUnIndex.Code != 400 || strings.TrimSpace(wUnIndex.Body.String()) != "Please pass POST/PUT/GET parameter value of 'path'." {
		t.Error("Expected code 400 and get message error not parameter 'path'")
	}
}
func TUnIndexErrorNotHave(t *testing.T) {
	setupTestCase()
	defer tearDownTestCase()

	b := &bytes.Buffer{}
	b.WriteString("{\"a\": 1, \"b\": 2}")

	reqCreate := httptest.NewRequest("GET", requestCreate, nil)
	reqInsert := httptest.NewRequest(RandMethodRequest(), requestInsertWithoutDoc, b)
	reqUnIndex := httptest.NewRequest(RandMethodRequest(), fmt.Sprintf(requestUnIndexes, collection, path), nil)

	wCreate := httptest.NewRecorder()
	wInsert := httptest.NewRecorder()
	wUnIndex := httptest.NewRecorder()

	var err error
	if HttpDB, err = db.OpenDB(tempDir); err != nil {
		panic(err)
	}
	Create(wCreate, reqCreate)
	Insert(wInsert, reqInsert)
	Unindex(wUnIndex, reqUnIndex)

	if wUnIndex.Code != 400 || strings.TrimSpace(wUnIndex.Body.String()) != fmt.Sprintf("Path [%s] is not indexed", path) {
		t.Error("Expected code 400 and get message error indexed not exist.")
	}
}
