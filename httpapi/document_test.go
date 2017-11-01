package httpapi

import (
	"bytes"
	"fmt"
	"github.com/HouzuoGuo/tiedot/data"
	"github.com/HouzuoGuo/tiedot/db"
	"math/rand"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
)

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func RandStringBytes(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
}

var (
	requestInsertWithoutDoc = fmt.Sprintf("http://localhost:8080/insert?col=%s", collection)
	requestInsertWithoutCol = "http://localhost:8080/insert"

	requestGet       = "http://localhost:8080/get?col=%s&id=%s"
	requestGetNotCol = "http://localhost:8080/get?id=%s"
	requestGetNotId  = "http://localhost:8080/get?col=%s"

	requestGetPageNotCol   = "http://localhost:8080/getpage"
	requestGetPageNotPage  = "http://localhost:8080/getpage?page=%s"
	requestGetPageNotTotal = "http://localhost:8080/getpage?col=%s&page=%s"
)

// Test Insert
func TestInsertNotExistParamCol(t *testing.T) {
	setupTestCase()
	defer tearDownTestCase()
	req := httptest.NewRequest("POST", requestInsertWithoutCol, nil)
	w := httptest.NewRecorder()

	var err error
	if HttpDB, err = db.OpenDB(tempDir); err != nil {
		panic(err)
	}
	Insert(w, req)

	if w.Code != 400 || strings.TrimSpace(w.Body.String()) != "Please pass POST/PUT/GET parameter value of 'col'." {
		t.Error("Expected code 400 and message error parameter col not exist")
	}
}
func TestInsertEmptyDoc(t *testing.T) {
	setupTestCase()
	defer tearDownTestCase()

	b := &bytes.Buffer{}
	b.WriteString("")
	reqCreate := httptest.NewRequest("POST", requestCreate, nil)
	reqInsert := httptest.NewRequest("POST", requestInsertWithoutDoc, b)
	wCreate := httptest.NewRecorder()
	wInsert := httptest.NewRecorder()

	var err error
	if HttpDB, err = db.OpenDB(tempDir); err != nil {
		panic(err)
	}
	Create(wCreate, reqCreate)
	Insert(wInsert, reqInsert)

	if wInsert.Code != 400 || strings.TrimSpace(wInsert.Body.String()) != "Please pass POST/PUT/GET parameter value of 'doc'." {
		t.Error("Expected code 400 and message error not exist parameter 'doc'")
	}
}
func TestInsertErrorUnmarshal(t *testing.T) {
	setupTestCase()
	defer tearDownTestCase()

	b := &bytes.Buffer{}
	b.WriteString("doc='{\"a\": 1, \"b\": 2}'")
	reqCreate := httptest.NewRequest("POST", requestCreate, nil)
	reqInsert := httptest.NewRequest("POST", requestInsertWithoutDoc, b)
	wCreate := httptest.NewRecorder()
	wInsert := httptest.NewRecorder()

	var err error
	if HttpDB, err = db.OpenDB(tempDir); err != nil {
		panic(err)
	}
	Create(wCreate, reqCreate)
	Insert(wInsert, reqInsert)

	if wInsert.Code != 400 || strings.TrimSpace(wInsert.Body.String()) != "'doc='{\"a\": 1, \"b\": 2}'' is not valid JSON document." {
		t.Error("Expected code 400 and message error not valid json")
	}
}
func TestInsert(t *testing.T) {
	setupTestCase()
	defer tearDownTestCase()

	b := &bytes.Buffer{}
	b.WriteString("{\"a\": 1, \"b\": 2}")

	reqCreate := httptest.NewRequest("GET", requestCreate, nil)
	reqInsert := httptest.NewRequest("POST", requestInsertWithoutDoc, b)
	wCreate := httptest.NewRecorder()
	wInsert := httptest.NewRecorder()

	var err error
	if HttpDB, err = db.OpenDB(tempDir); err != nil {
		panic(err)
	}
	Create(wCreate, reqCreate)
	Insert(wInsert, reqInsert)

	_, err = strconv.Atoi(strings.TrimSpace(wInsert.Body.String()))

	if wInsert.Code != 201 || err != nil {
		t.Error("Expected code 201 and get id new record")
	}
}
func TestInsertCollectionNotExist(t *testing.T) {
	setupTestCase()
	defer tearDownTestCase()

	b := &bytes.Buffer{}
	b.WriteString("{\"a\": 1, \"b\": 2}")

	reqInsert := httptest.NewRequest("POST", requestInsertWithoutDoc, b)
	wInsert := httptest.NewRecorder()

	var err error
	if HttpDB, err = db.OpenDB(tempDir); err != nil {
		panic(err)
	}
	Insert(wInsert, reqInsert)

	if wInsert.Code != 400 || strings.TrimSpace(wInsert.Body.String()) != fmt.Sprintf("Collection '%s' does not exist.", collection) {
		t.Error("Expected code 400 and message error not exist is collection.")
	}
}
func TestInsertError(t *testing.T) {
	setupTestCase()
	defer tearDownTestCase()

	sizeByte := data.DOC_MAX_ROOM
	stringJson := fmt.Sprintf("{\"a\": 1, \"b\": \"%s\"}", RandStringBytes(sizeByte))
	b := bytes.NewBuffer([]byte(stringJson))
	reqCreate := httptest.NewRequest("GET", requestCreate, nil)
	reqInsert := httptest.NewRequest("POST", requestInsertWithoutDoc, b)
	wInsert := httptest.NewRecorder()
	wCreate := httptest.NewRecorder()

	var err error
	if HttpDB, err = db.OpenDB(tempDir); err != nil {
		panic(err)
	}
	Create(wCreate, reqCreate)
	Insert(wInsert, reqInsert)

	if wInsert.Code != 500 || strings.TrimSpace(wInsert.Body.String()) != "Document is too large. Max: `2097152`, Given: `4194332`" {
		t.Error("Expected code 500 and message document is too large.")
	}
}

// Test Get
func TestGet(t *testing.T) {
	setupTestCase()
	defer tearDownTestCase()
	var err error
	if HttpDB, err = db.OpenDB(tempDir); err != nil {
		panic(err)
	}

	b := &bytes.Buffer{}
	jsonStr := "{\"a\":1,\"b\":2}"
	b.WriteString(jsonStr)

	reqCreate := httptest.NewRequest("GET", requestCreate, nil)
	reqInsert := httptest.NewRequest("POST", requestInsertWithoutDoc, b)

	wCreate := httptest.NewRecorder()
	wInsert := httptest.NewRecorder()
	wGet := httptest.NewRecorder()

	Create(wCreate, reqCreate)
	Insert(wInsert, reqInsert)

	reqGet := httptest.NewRequest("POST", fmt.Sprintf(requestGet, collection, strings.TrimSpace(wInsert.Body.String())), b)

	Get(wGet, reqGet)

	if wGet.Code != 200 || strings.TrimSpace(wGet.Body.String()) != strings.Replace(jsonStr, "\\", "", -1) {
		t.Error("Expected code 200 and get document from collection")
	}
}
func TestGetInvalidId(t *testing.T) {
	setupTestCase()
	defer tearDownTestCase()
	var err error
	if HttpDB, err = db.OpenDB(tempDir); err != nil {
		panic(err)
	}

	b := &bytes.Buffer{}
	jsonStr := "{\"a\":1,\"b\":2}"
	b.WriteString(jsonStr)

	reqCreate := httptest.NewRequest("GET", requestCreate, nil)

	wCreate := httptest.NewRecorder()
	wGet := httptest.NewRecorder()

	Create(wCreate, reqCreate)

	randIntStr := RandStringBytes(rand.Intn(5))

	reqGet := httptest.NewRequest("POST", fmt.Sprintf(requestGet, collection, randIntStr), b)
	Get(wGet, reqGet)

	if wGet.Code != 400 || strings.TrimSpace(wGet.Body.String()) != fmt.Sprintf("Invalid document ID '%s'.", randIntStr) {
		t.Error("Expected code 404 and message error invalid document ID")
	}
}
func TestGetCollectionNotExist(t *testing.T) {
	setupTestCase()
	defer tearDownTestCase()

	var err error
	randIntStr := strconv.Itoa(rand.Int())
	collectionFake := "fake"
	if HttpDB, err = db.OpenDB(tempDir); err != nil {
		panic(err)
	}

	b := &bytes.Buffer{}
	jsonStr := "{\"a\":1,\"b\":2}"
	b.WriteString(jsonStr)

	reqCreate := httptest.NewRequest("GET", requestCreate, nil)
	reqGet := httptest.NewRequest("POST", fmt.Sprintf(requestGet, collectionFake, randIntStr), b)
	wCreate := httptest.NewRecorder()
	wGet := httptest.NewRecorder()

	Create(wCreate, reqCreate)
	Get(wGet, reqGet)

	if wGet.Code != 400 || strings.TrimSpace(wGet.Body.String()) != fmt.Sprintf("Collection '%s' does not exist.", collectionFake) {
		t.Error("Expected code 400 and message error not exist collection")
	}
}
func TestGetNoSuchDocument(t *testing.T) {
	setupTestCase()
	defer tearDownTestCase()

	var err error
	randIntStr := strconv.Itoa(rand.Int())

	if HttpDB, err = db.OpenDB(tempDir); err != nil {
		panic(err)
	}

	b := &bytes.Buffer{}
	jsonStr := "{\"a\":1,\"b\":2}"
	b.WriteString(jsonStr)

	reqCreate := httptest.NewRequest("GET", requestCreate, nil)
	reqGet := httptest.NewRequest("POST", fmt.Sprintf(requestGet, collection, randIntStr), b)
	wCreate := httptest.NewRecorder()
	wGet := httptest.NewRecorder()

	Create(wCreate, reqCreate)
	Get(wGet, reqGet)

	if wGet.Code != 404 || strings.TrimSpace(wGet.Body.String()) != fmt.Sprintf("No such document ID %s.", randIntStr) {
		t.Error("Expected code 404 and message error not such document")
	}
}
func TestGetNotParamCol(t *testing.T) {
	setupTestCase()
	defer tearDownTestCase()

	var err error
	randIntStr := strconv.Itoa(rand.Int())

	if HttpDB, err = db.OpenDB(tempDir); err != nil {
		panic(err)
	}
	reqCreate := httptest.NewRequest("GET", requestCreate, nil)
	reqGet := httptest.NewRequest("POST", fmt.Sprintf(requestGetNotCol, randIntStr), nil)
	wCreate := httptest.NewRecorder()
	wGet := httptest.NewRecorder()

	Create(wCreate, reqCreate)
	Get(wGet, reqGet)

	if wGet.Code != 400 || strings.TrimSpace(wGet.Body.String()) != "Please pass POST/PUT/GET parameter value of 'col'." {
		t.Error("Expected code 400 and message error not such param 'col'")
	}
}
func TestGetNotParamId(t *testing.T) {
	setupTestCase()
	defer tearDownTestCase()
	var err error

	if HttpDB, err = db.OpenDB(tempDir); err != nil {
		panic(err)
	}

	reqCreate := httptest.NewRequest("GET", requestCreate, nil)
	reqGet := httptest.NewRequest("POST", fmt.Sprintf(requestGetNotId, collection), nil)
	wCreate := httptest.NewRecorder()
	wGet := httptest.NewRecorder()

	Create(wCreate, reqCreate)
	Get(wGet, reqGet)

	if wGet.Code != 400 || strings.TrimSpace(wGet.Body.String()) != "Please pass POST/PUT/GET parameter value of 'id'." {
		t.Error("Expected code 400 and message error not such param 'id'")
	}
}

// Test GetPage
func TestGetPage(t *testing.T) {
	//requestGetPageNotCol
	setupTestCase()
	defer tearDownTestCase()

	b := &bytes.Buffer{}
	b.WriteString("{\"a\": 1, \"b\": 2}")

	reqCreate := httptest.NewRequest("GET", requestCreate, nil)
	reqInsert := httptest.NewRequest("POST", requestInsertWithoutDoc, b)
	wCreate := httptest.NewRecorder()
	wInsert := httptest.NewRecorder()

	var err error
	if HttpDB, err = db.OpenDB(tempDir); err != nil {
		panic(err)
	}
	Create(wCreate, reqCreate)
	Insert(wInsert, reqInsert)

	_, err = strconv.Atoi(strings.TrimSpace(wInsert.Body.String()))

	if wInsert.Code != 201 || err != nil {
		t.Error("Expected code 201 and get id new record")
	}
}
