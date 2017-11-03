package httpapi

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/HouzuoGuo/tiedot/data"
	"github.com/HouzuoGuo/tiedot/db"
	"github.com/bouk/monkey"
	"math/rand"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
)

var (
	requestInsertWithoutDoc = fmt.Sprintf("http://localhost:8080/insert?col=%s", collection)
	requestInsertWithoutCol = "http://localhost:8080/insert"

	requestGet       = "http://localhost:8080/get?col=%s&id=%s"
	requestGetNotCol = "http://localhost:8080/get?id=%s"
	requestGetNotId  = "http://localhost:8080/get?col=%s"

	requestGetPageNotCol   = "http://localhost:8080/getpage"
	requestGetPageNotPage  = "http://localhost:8080/getpage?col=%s"
	requestGetPageNotTotal = "http://localhost:8080/getpage?col=%s&page=%s"
	requestGetPage         = "http://localhost:8080/getpage?col=%s&page=%s&total=%d"

	requestUpdateNotCol = "http://localhost:8080/update"
	requestUpdateNotId  = "http://localhost:8080/update?col=%s"
	requestUpdateNotDoc = "http://localhost:8080/update?col=%s&id=%s"
	requestUpdate       = "http://localhost:8080/update?col=%s&id=%s"

	requestDeleteNotCol = "http://localhost:8080/delete"
	requestDeleteNotId  = "http://localhost:8080/delete?col=%s"
	requestDelete       = "http://localhost:8080/delete?col=%s&id=%s"

	requestApproxDocCountNotCol = "http://localhost:8080/approxdoccount"
	requestApproxDocCount       = "http://localhost:8080/approxdoccount?col=%s"

	page  = "1"
	total = 2
)

// General function run tests document
func TestDocument(t *testing.T) {
	testsDocument := []func(t *testing.T){
		TInsertNotExistParamCol,
		TInsertEmptyDoc,
		TInsertErrorUnmarshal,
		TInsert,
		TInsertCollectionNotExist,
		TInsertError,
		TGet,
		TGetMarshalError,
		TGetInvalidId,
		TGetCollectionNotExist,
		TGetNoSuchDocument,
		TGetNotParamCol,
		TGetNotParamId,
		TGetPageNotCol,
		TGetPageNotPage,
		TGetPageNotTotal,
		TGetPageErrorTotal,
		TGetPageErrorPage,
		TGetPageCollectionNotExist,
		TGetPage,
		TGetPageErrMarshalJson,
		TUpdateNotCol,
		TUpdateNotId,
		TUpdateNotDoc,
		TUpdateInvalidId,
		TUpdateJsonError,
		TUpdateCollectionNotExist,
		TUpdate,
		TUpdateError,
		TDeleteNotCol,
		TDeleteNotId,
		TDeleteInvalidId,
		TDeleteCollectionNotExist,
		TDelete,
		TApproxDocCountNotCol,
		TApproxDocCountColNotExist,
		TApproxDocCount,
	}
	managerSubTests(testsDocument, "document_test", t)
}

// Test Insert
func TInsertNotExistParamCol(t *testing.T) {
	setupTestCase()
	defer tearDownTestCase()
	req := httptest.NewRequest(RandMethodRequest(), requestInsertWithoutCol, nil)
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
func TInsertEmptyDoc(t *testing.T) {
	setupTestCase()
	defer tearDownTestCase()

	b := &bytes.Buffer{}
	b.WriteString("")
	reqCreate := httptest.NewRequest(RandMethodRequest(), requestCreate, nil)
	reqInsert := httptest.NewRequest(RandMethodRequest(), requestInsertWithoutDoc, b)
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
func TInsertErrorUnmarshal(t *testing.T) {
	setupTestCase()
	defer tearDownTestCase()

	b := &bytes.Buffer{}
	b.WriteString("doc='{\"a\": 1, \"b\": 2}'")
	reqCreate := httptest.NewRequest(RandMethodRequest(), requestCreate, nil)
	reqInsert := httptest.NewRequest(RandMethodRequest(), requestInsertWithoutDoc, b)
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
func TInsert(t *testing.T) {
	setupTestCase()
	defer tearDownTestCase()

	b := &bytes.Buffer{}
	b.WriteString("{\"a\": 1, \"b\": 2}")

	reqCreate := httptest.NewRequest("GET", requestCreate, nil)
	reqInsert := httptest.NewRequest(RandMethodRequest(), requestInsertWithoutDoc, b)
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
func TInsertCollectionNotExist(t *testing.T) {
	setupTestCase()
	defer tearDownTestCase()

	b := &bytes.Buffer{}
	b.WriteString("{\"a\": 1, \"b\": 2}")

	reqInsert := httptest.NewRequest(RandMethodRequest(), requestInsertWithoutDoc, b)
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
func TInsertError(t *testing.T) {
	setupTestCase()
	defer tearDownTestCase()

	sizeByte := data.DOC_MAX_ROOM
	stringJson := fmt.Sprintf("{\"a\": 1, \"b\": \"%s\"}", RandStringBytes(sizeByte))
	b := bytes.NewBuffer([]byte(stringJson))
	reqCreate := httptest.NewRequest(RandMethodRequest(), requestCreate, nil)
	reqInsert := httptest.NewRequest(RandMethodRequest(), requestInsertWithoutDoc, b)
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
func TGet(t *testing.T) {
	setupTestCase()
	defer tearDownTestCase()
	var err error
	if HttpDB, err = db.OpenDB(tempDir); err != nil {
		panic(err)
	}

	b := &bytes.Buffer{}
	jsonStr := "{\"a\":1,\"b\":2}"
	b.WriteString(jsonStr)

	reqCreate := httptest.NewRequest(RandMethodRequest(), requestCreate, nil)
	reqInsert := httptest.NewRequest(RandMethodRequest(), requestInsertWithoutDoc, b)

	wCreate := httptest.NewRecorder()
	wInsert := httptest.NewRecorder()
	wGet := httptest.NewRecorder()

	Create(wCreate, reqCreate)
	Insert(wInsert, reqInsert)

	reqGet := httptest.NewRequest(RandMethodRequest(), fmt.Sprintf(requestGet, collection, strings.TrimSpace(wInsert.Body.String())), nil)

	Get(wGet, reqGet)

	if wGet.Code != 200 || strings.TrimSpace(wGet.Body.String()) != strings.Replace(jsonStr, "\\", "", -1) {
		t.Error("Expected code 200 and get document from collection")
	}
}
func TGetInvalidId(t *testing.T) {
	setupTestCase()
	defer tearDownTestCase()
	var err error
	if HttpDB, err = db.OpenDB(tempDir); err != nil {
		panic(err)
	}

	b := &bytes.Buffer{}
	jsonStr := "{\"a\":1,\"b\":2}"
	b.WriteString(jsonStr)

	wGet := httptest.NewRecorder()

	randIntStr := RandStringBytes(5)
	reqGet := httptest.NewRequest(RandMethodRequest(), fmt.Sprintf(requestGet, collection, randIntStr), b)
	Get(wGet, reqGet)

	if wGet.Code != 400 || strings.TrimSpace(wGet.Body.String()) != fmt.Sprintf("Invalid document ID '%s'.", randIntStr) {
		t.Error("Expected code 404 and message error invalid document ID")
	}
}
func TGetCollectionNotExist(t *testing.T) {
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

	reqCreate := httptest.NewRequest(RandMethodRequest(), requestCreate, nil)
	reqGet := httptest.NewRequest(RandMethodRequest(), fmt.Sprintf(requestGet, collectionFake, randIntStr), b)
	wCreate := httptest.NewRecorder()
	wGet := httptest.NewRecorder()

	Create(wCreate, reqCreate)
	Get(wGet, reqGet)

	if wGet.Code != 400 || strings.TrimSpace(wGet.Body.String()) != fmt.Sprintf("Collection '%s' does not exist.", collectionFake) {
		t.Error("Expected code 400 and message error not exist collection")
	}
}
func TGetNoSuchDocument(t *testing.T) {
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

	reqCreate := httptest.NewRequest(RandMethodRequest(), requestCreate, nil)
	reqGet := httptest.NewRequest(RandMethodRequest(), fmt.Sprintf(requestGet, collection, randIntStr), b)
	wCreate := httptest.NewRecorder()
	wGet := httptest.NewRecorder()

	Create(wCreate, reqCreate)
	Get(wGet, reqGet)

	if wGet.Code != 404 || strings.TrimSpace(wGet.Body.String()) != fmt.Sprintf("No such document ID %s.", randIntStr) {
		t.Error("Expected code 404 and message error not such document")
	}
}
func TGetNotParamCol(t *testing.T) {
	setupTestCase()
	defer tearDownTestCase()

	var err error
	randIntStr := strconv.Itoa(rand.Int())

	if HttpDB, err = db.OpenDB(tempDir); err != nil {
		panic(err)
	}
	reqGet := httptest.NewRequest(RandMethodRequest(), fmt.Sprintf(requestGetNotCol, randIntStr), nil)
	wGet := httptest.NewRecorder()

	Get(wGet, reqGet)

	if wGet.Code != 400 || strings.TrimSpace(wGet.Body.String()) != "Please pass POST/PUT/GET parameter value of 'col'." {
		t.Error("Expected code 400 and message error not such param 'col'")
	}
}
func TGetNotParamId(t *testing.T) {
	setupTestCase()
	defer tearDownTestCase()
	var err error

	if HttpDB, err = db.OpenDB(tempDir); err != nil {
		panic(err)
	}
	reqGet := httptest.NewRequest(RandMethodRequest(), fmt.Sprintf(requestGetNotId, collection), nil)
	wGet := httptest.NewRecorder()
	Get(wGet, reqGet)

	if wGet.Code != 400 || strings.TrimSpace(wGet.Body.String()) != "Please pass POST/PUT/GET parameter value of 'id'." {
		t.Error("Expected code 400 and message error not such param 'id'")
	}
}
func TGetMarshalError(t *testing.T) {
	setupTestCase()
	defer tearDownTestCase()
	var err error
	if HttpDB, err = db.OpenDB(tempDir); err != nil {
		panic(err)
	}

	b := &bytes.Buffer{}
	jsonStr := "{\"a\":1,\"b\":2}"
	b.WriteString(jsonStr)

	reqCreate := httptest.NewRequest(RandMethodRequest(), requestCreate, nil)
	reqInsert := httptest.NewRequest(RandMethodRequest(), requestInsertWithoutDoc, b)

	wCreate := httptest.NewRecorder()
	wInsert := httptest.NewRecorder()
	wGet := httptest.NewRecorder()

	Create(wCreate, reqCreate)
	Insert(wInsert, reqInsert)

	textError := "Json marshal error"
	reqGet := httptest.NewRequest(RandMethodRequest(), fmt.Sprintf(requestGet, collection, strings.TrimSpace(wInsert.Body.String())), nil)
	patch := monkey.Patch(json.Marshal, func(interface{}) ([]byte, error) {
		return nil, errors.New(textError)
	})
	defer patch.Unpatch()
	Get(wGet, reqGet)

	if wGet.Code != 500 || strings.TrimSpace(wGet.Body.String()) != textError {
		t.Error("Expected code 500 and json marshal error.")
	}
}

// Test GetPage
func TGetPageNotCol(t *testing.T) {
	setupTestCase()
	defer tearDownTestCase()

	reqGetPage := httptest.NewRequest(RandMethodRequest(), requestGetPageNotCol, nil)
	wGetPage := httptest.NewRecorder()

	var err error
	if HttpDB, err = db.OpenDB(tempDir); err != nil {
		panic(err)
	}

	GetPage(wGetPage, reqGetPage)

	if wGetPage.Code != 400 || strings.TrimSpace(wGetPage.Body.String()) != "Please pass POST/PUT/GET parameter value of 'col'." {
		t.Error("Expected code 400 and message error value of 'col'")
	}
}
func TGetPageNotPage(t *testing.T) {
	setupTestCase()
	defer tearDownTestCase()

	reqGetPage := httptest.NewRequest(RandMethodRequest(), fmt.Sprintf(requestGetPageNotPage, collection), nil)
	wGetPage := httptest.NewRecorder()

	var err error
	if HttpDB, err = db.OpenDB(tempDir); err != nil {
		panic(err)
	}

	GetPage(wGetPage, reqGetPage)

	if wGetPage.Code != 400 || strings.TrimSpace(wGetPage.Body.String()) != "Please pass POST/PUT/GET parameter value of 'page'." {
		t.Error("Expected code 400 and message error value of 'page'")
	}
}
func TGetPageNotTotal(t *testing.T) {
	setupTestCase()
	defer tearDownTestCase()

	reqGetPage := httptest.NewRequest(RandMethodRequest(), fmt.Sprintf(requestGetPageNotTotal, collection, page), nil)
	wGetPage := httptest.NewRecorder()

	var err error
	if HttpDB, err = db.OpenDB(tempDir); err != nil {
		panic(err)
	}

	GetPage(wGetPage, reqGetPage)

	if wGetPage.Code != 400 || strings.TrimSpace(wGetPage.Body.String()) != "Please pass POST/PUT/GET parameter value of 'total'." {
		t.Error("Expected code 400 and message error value of 'total'")
	}
}
func TGetPageErrorTotal(t *testing.T) {
	setupTestCase()
	defer tearDownTestCase()

	reqGetPage := httptest.NewRequest(RandMethodRequest(), fmt.Sprintf(requestGetPage, collection, page, 0), nil)
	wGetPage := httptest.NewRecorder()

	var err error
	if HttpDB, err = db.OpenDB(tempDir); err != nil {
		panic(err)
	}

	GetPage(wGetPage, reqGetPage)

	if wGetPage.Code != 400 || strings.TrimSpace(wGetPage.Body.String()) != "Invalid total page number '0'." {
		t.Error("Expected code 400 and message error invalid total page number 0")
	}
}
func TGetPageErrorPage(t *testing.T) {
	setupTestCase()
	defer tearDownTestCase()

	reqGetPage := httptest.NewRequest(RandMethodRequest(), fmt.Sprintf(requestGetPage, collection, "-1", total), nil)
	wGetPage := httptest.NewRecorder()

	var err error
	if HttpDB, err = db.OpenDB(tempDir); err != nil {
		panic(err)
	}

	GetPage(wGetPage, reqGetPage)

	if wGetPage.Code != 400 || strings.TrimSpace(wGetPage.Body.String()) != "Invalid page number '-1'." {
		t.Error("Expected code 400 and message error invalid page number -1")
	}
}
func TGetPageCollectionNotExist(t *testing.T) {
	setupTestCase()
	defer tearDownTestCase()

	b := &bytes.Buffer{}
	b.WriteString("{\"a\": 1, \"b\": 2}")

	reqGetPage := httptest.NewRequest(RandMethodRequest(), fmt.Sprintf(requestGetPage, collection, page, total), nil)
	wGetPage := httptest.NewRecorder()

	var err error
	if HttpDB, err = db.OpenDB(tempDir); err != nil {
		panic(err)
	}

	GetPage(wGetPage, reqGetPage)

	if wGetPage.Code != 400 || strings.TrimSpace(wGetPage.Body.String()) != fmt.Sprintf("Collection '%s' does not exist.", collection) {
		t.Error("Expected code 400 and message error collection does not exist.")
	}
}
func TGetPage(t *testing.T) {
	setupTestCase()
	defer tearDownTestCase()

	b := &bytes.Buffer{}
	b.WriteString("{\"a\": 1, \"b\": 2}")

	reqCreate := httptest.NewRequest(RandMethodRequest(), requestCreate, nil)
	reqInsert := httptest.NewRequest(RandMethodRequest(), requestInsertWithoutDoc, b)
	reqGetPage := httptest.NewRequest(RandMethodRequest(), fmt.Sprintf(requestGetPage, collection, page, total), nil)

	wCreate := httptest.NewRecorder()
	wInsert := httptest.NewRecorder()
	wGetPage := httptest.NewRecorder()

	var err error
	if HttpDB, err = db.OpenDB(tempDir); err != nil {
		panic(err)
	}

	Create(wCreate, reqCreate)
	Insert(wInsert, reqInsert)

	GetPage(wGetPage, reqGetPage)

	if wGetPage.Code != 200 {
		t.Error("Expected code 200 and json data")
	}
}
func TGetPageErrMarshalJson(t *testing.T) {
	setupTestCase()
	defer tearDownTestCase()

	b := &bytes.Buffer{}
	b.WriteString("{\"a\": 1, \"b\": 2}")

	reqCreate := httptest.NewRequest(RandMethodRequest(), requestCreate, nil)
	reqInsert := httptest.NewRequest(RandMethodRequest(), requestInsertWithoutDoc, b)
	reqGetPage := httptest.NewRequest(RandMethodRequest(), fmt.Sprintf(requestGetPage, collection, page, total), nil)

	wCreate := httptest.NewRecorder()
	wInsert := httptest.NewRecorder()
	wGetPage := httptest.NewRecorder()

	var err error
	if HttpDB, err = db.OpenDB(tempDir); err != nil {
		panic(err)
	}

	Create(wCreate, reqCreate)
	Insert(wInsert, reqInsert)
	textError := "Json marshal error"
	patch := monkey.Patch(json.Marshal, func(interface{}) ([]byte, error) {
		return nil, errors.New(textError)
	})
	defer patch.Unpatch()
	GetPage(wGetPage, reqGetPage)

	if wGetPage.Code != 500 || strings.TrimSpace(wGetPage.Body.String()) != textError {
		t.Error("Expected code 500 and message error json marshal.")
	}
}

// Test Update
func TUpdateNotCol(t *testing.T) {
	setupTestCase()
	defer tearDownTestCase()

	reqUpdate := httptest.NewRequest(RandMethodRequest(), requestUpdateNotCol, nil)
	wUpdate := httptest.NewRecorder()

	var err error
	if HttpDB, err = db.OpenDB(tempDir); err != nil {
		panic(err)
	}

	Update(wUpdate, reqUpdate)
	if wUpdate.Code != 400 || strings.TrimSpace(wUpdate.Body.String()) != "Please pass POST/PUT/GET parameter value of 'col'." {
		t.Error("Expected code 400 and message error value of 'col'")
	}
}
func TUpdateNotId(t *testing.T) {
	setupTestCase()
	defer tearDownTestCase()

	reqUpdate := httptest.NewRequest(RandMethodRequest(), fmt.Sprintf(requestUpdateNotId, collection), nil)
	wUpdate := httptest.NewRecorder()

	var err error
	if HttpDB, err = db.OpenDB(tempDir); err != nil {
		panic(err)
	}

	Update(wUpdate, reqUpdate)
	if wUpdate.Code != 400 || strings.TrimSpace(wUpdate.Body.String()) != "Please pass POST/PUT/GET parameter value of 'id'." {
		t.Error("Expected code 400 and message error value of 'id'")
	}
}
func TUpdateNotDoc(t *testing.T) {
	setupTestCase()
	defer tearDownTestCase()

	wUpdate := httptest.NewRecorder()

	var err error
	if HttpDB, err = db.OpenDB(tempDir); err != nil {
		panic(err)
	}

	reqUpdate := httptest.NewRequest(RandMethodRequest(), fmt.Sprintf(requestUpdateNotDoc, collection, "1"), nil)
	Update(wUpdate, reqUpdate)

	if wUpdate.Code != 400 || strings.TrimSpace(wUpdate.Body.String()) != "Please pass POST/PUT/GET parameter value of 'doc'." {
		t.Error("Expected code 400 and message error value of 'doc'")
	}
}
func TUpdateInvalidId(t *testing.T) {
	setupTestCase()
	defer tearDownTestCase()

	wUpdate := httptest.NewRecorder()

	var err error
	if HttpDB, err = db.OpenDB(tempDir); err != nil {
		panic(err)
	}

	b := &bytes.Buffer{}
	jsonStr := "{\"a\":1,\"b\":2}"
	b.WriteString(jsonStr)

	randId := RandStringBytes(5)
	reqUpdate := httptest.NewRequest(RandMethodRequest(), fmt.Sprintf(requestUpdate, collection, randId), b)
	Update(wUpdate, reqUpdate)

	if wUpdate.Code != 400 || strings.TrimSpace(wUpdate.Body.String()) != fmt.Sprintf("Invalid document ID '%s'.", randId) {
		t.Error("Expected code 400 and message error invalid document id.")
	}
}
func TUpdateJsonError(t *testing.T) {
	setupTestCase()
	defer tearDownTestCase()

	wUpdate := httptest.NewRecorder()
	var err error
	if HttpDB, err = db.OpenDB(tempDir); err != nil {
		panic(err)
	}

	b := &bytes.Buffer{}
	jsonStr := "{\"a\":1,\"b\":asd}"
	b.WriteString(jsonStr)

	reqUpdate := httptest.NewRequest(RandMethodRequest(), fmt.Sprintf(requestUpdate, collection, "1"), b)
	Update(wUpdate, reqUpdate)

	if wUpdate.Code != 400 || strings.TrimSpace(wUpdate.Body.String()) != "'map[]' is not valid JSON document." {
		t.Error("Expected code 400 and message error is not valid json")
	}
}
func TUpdateCollectionNotExist(t *testing.T) {
	setupTestCase()
	defer tearDownTestCase()

	wUpdate := httptest.NewRecorder()
	var err error
	if HttpDB, err = db.OpenDB(tempDir); err != nil {
		panic(err)
	}

	b := &bytes.Buffer{}
	jsonStr := "{\"a\":1,\"b\":2}"
	b.WriteString(jsonStr)

	reqUpdate := httptest.NewRequest(RandMethodRequest(), fmt.Sprintf(requestUpdate, collection, "1"), b)
	Update(wUpdate, reqUpdate)

	if wUpdate.Code != 400 || strings.TrimSpace(wUpdate.Body.String()) != fmt.Sprintf("Collection '%s' does not exist.", collection) {
		t.Error("Expected code 400 and message error collection is not exist")
	}
}
func TUpdate(t *testing.T) {
	setupTestCase()
	defer tearDownTestCase()
	jsonStr := "{\"a\":1,\"b\":2}"
	jsonStrForUpdate := "{\"a\":1,\"b\":3}"

	b := &bytes.Buffer{}
	b.WriteString(jsonStr)

	b2 := &bytes.Buffer{}
	b2.WriteString(jsonStrForUpdate)

	reqCreate := httptest.NewRequest(RandMethodRequest(), requestCreate, nil)
	reqInsert := httptest.NewRequest(RandMethodRequest(), requestInsertWithoutDoc, b)

	wCreate := httptest.NewRecorder()
	wInsert := httptest.NewRecorder()
	wUpdate := httptest.NewRecorder()
	wGet := httptest.NewRecorder()

	var err error
	if HttpDB, err = db.OpenDB(tempDir); err != nil {
		panic(err)
	}

	Create(wCreate, reqCreate)
	Insert(wInsert, reqInsert)

	reqUpdate := httptest.NewRequest(RandMethodRequest(), fmt.Sprintf(requestUpdate, collection, strings.TrimSpace(wInsert.Body.String())), b2)
	Update(wUpdate, reqUpdate)

	reqGet := httptest.NewRequest(RandMethodRequest(), fmt.Sprintf(requestGet, collection, strings.TrimSpace(wInsert.Body.String())), b)
	Get(wGet, reqGet)

	if wUpdate.Code != 200 || wGet.Code != 200 || strings.TrimSpace(wGet.Body.String()) != "{\"a\":1,\"b\":3}" {
		t.Error("Expected code 200 and get update document")
	}
}
func TUpdateError(t *testing.T) {
	setupTestCase()
	defer tearDownTestCase()

	jsonStrForUpdate := "{\"a\":1,\"b\":3}"

	b2 := &bytes.Buffer{}
	b2.WriteString(jsonStrForUpdate)

	reqCreate := httptest.NewRequest(RandMethodRequest(), requestCreate, nil)
	wCreate := httptest.NewRecorder()
	wUpdate := httptest.NewRecorder()

	var err error
	if HttpDB, err = db.OpenDB(tempDir); err != nil {
		panic(err)
	}

	Create(wCreate, reqCreate)

	reqUpdate := httptest.NewRequest(RandMethodRequest(), fmt.Sprintf(requestUpdate, collection, "2"), b2)
	Update(wUpdate, reqUpdate)

	if wUpdate.Code != 500 || strings.TrimSpace(wUpdate.Body.String()) != "Document `2` does not exist" {
		t.Error("Expected code 500 and message error document not exist")
	}
}

//Test Delete
func TDeleteNotCol(t *testing.T) {
	setupTestCase()
	defer tearDownTestCase()

	reqDelete := httptest.NewRequest(RandMethodRequest(), requestDeleteNotCol, nil)
	wDelete := httptest.NewRecorder()

	var err error
	if HttpDB, err = db.OpenDB(tempDir); err != nil {
		panic(err)
	}

	Delete(wDelete, reqDelete)
	if wDelete.Code != 400 || strings.TrimSpace(wDelete.Body.String()) != "Please pass POST/PUT/GET parameter value of 'col'." {
		t.Error("Expected code 400 and message error value of 'col'")
	}
}
func TDeleteNotId(t *testing.T) {
	setupTestCase()
	defer tearDownTestCase()

	reqDelete := httptest.NewRequest(RandMethodRequest(), fmt.Sprintf(requestDeleteNotId, collection), nil)
	wDelete := httptest.NewRecorder()

	var err error
	if HttpDB, err = db.OpenDB(tempDir); err != nil {
		panic(err)
	}
	Delete(wDelete, reqDelete)

	if wDelete.Code != 400 || strings.TrimSpace(wDelete.Body.String()) != "Please pass POST/PUT/GET parameter value of 'id'." {
		t.Error("Expected code 400 and message error value of 'id'")
	}
}
func TDeleteInvalidId(t *testing.T) {
	setupTestCase()
	defer tearDownTestCase()
	randId := RandStringBytes(5)
	reqDelete := httptest.NewRequest(RandMethodRequest(), fmt.Sprintf(requestDelete, collection, randId), nil)
	wDelete := httptest.NewRecorder()

	var err error
	if HttpDB, err = db.OpenDB(tempDir); err != nil {
		panic(err)
	}
	Delete(wDelete, reqDelete)

	if wDelete.Code != 400 || strings.TrimSpace(wDelete.Body.String()) != fmt.Sprintf("Invalid document ID '%s'.", randId) {
		t.Error("Expected code 400 and message error invalid document id.")
	}
}
func TDeleteCollectionNotExist(t *testing.T) {
	setupTestCase()
	defer tearDownTestCase()

	reqDelete := httptest.NewRequest(RandMethodRequest(), fmt.Sprintf(requestDelete, collection, "1"), nil)
	wDelete := httptest.NewRecorder()
	var err error
	if HttpDB, err = db.OpenDB(tempDir); err != nil {
		panic(err)
	}
	Delete(wDelete, reqDelete)

	if wDelete.Code != 400 || strings.TrimSpace(wDelete.Body.String()) != fmt.Sprintf("Collection '%s' does not exist.", collection) {
		t.Error("Expected code 400 and message error collection does not exist.")
	}
}
func TDelete(t *testing.T) {
	setupTestCase()
	defer tearDownTestCase()
	b := &bytes.Buffer{}
	jsonStr := "{\"a\":1,\"b\":2}"
	b.WriteString(jsonStr)

	reqCreate := httptest.NewRequest(RandMethodRequest(), requestCreate, nil)
	reqInsert := httptest.NewRequest(RandMethodRequest(), requestInsertWithoutDoc, b)

	wDelete := httptest.NewRecorder()
	wCreate := httptest.NewRecorder()
	wInsert := httptest.NewRecorder()
	wGet := httptest.NewRecorder()

	var err error
	if HttpDB, err = db.OpenDB(tempDir); err != nil {
		panic(err)
	}
	Create(wCreate, reqCreate)
	Insert(wInsert, reqInsert)
	idRecord := strings.TrimSpace(wInsert.Body.String())
	reqDelete := httptest.NewRequest(RandMethodRequest(), fmt.Sprintf(requestDelete, collection, idRecord), nil)
	reqGet := httptest.NewRequest(RandMethodRequest(), fmt.Sprintf(requestGet, collection, strings.TrimSpace(wInsert.Body.String())), nil)
	Delete(wDelete, reqDelete)
	Get(wGet, reqGet)

	if wDelete.Code != 200 || wGet.Code != 404 || strings.TrimSpace(wGet.Body.String()) != fmt.Sprintf("No such document ID %s.", idRecord) {
		t.Error("Expected code 200 and after delete message error not such document with the specified 'id'")
	}
}

// Test ApproxDocCount
func TApproxDocCountNotCol(t *testing.T) {
	setupTestCase()
	defer tearDownTestCase()

	reqApproxDocCount := httptest.NewRequest(RandMethodRequest(), requestApproxDocCountNotCol, nil)
	wApproxDocCount := httptest.NewRecorder()

	var err error
	if HttpDB, err = db.OpenDB(tempDir); err != nil {
		panic(err)
	}

	ApproxDocCount(wApproxDocCount, reqApproxDocCount)

	if wApproxDocCount.Code != 400 || strings.TrimSpace(wApproxDocCount.Body.String()) != "Please pass POST/PUT/GET parameter value of 'col'." {
		t.Error("Expected code 400 and message error value of 'col'")
	}
}
func TApproxDocCountColNotExist(t *testing.T) {
	setupTestCase()
	defer tearDownTestCase()

	reqApproxDocCount := httptest.NewRequest(RandMethodRequest(), fmt.Sprintf(requestApproxDocCount, collection), nil)
	wApproxDocCount := httptest.NewRecorder()

	var err error
	if HttpDB, err = db.OpenDB(tempDir); err != nil {
		panic(err)
	}

	ApproxDocCount(wApproxDocCount, reqApproxDocCount)

	if wApproxDocCount.Code != 400 || strings.TrimSpace(wApproxDocCount.Body.String()) != fmt.Sprintf("Collection '%s' does not exist.", collection) {
		t.Error("Expected code 400 and message error collection does not exist.")
	}
}
func TApproxDocCount(t *testing.T) {
	setupTestCase()
	defer tearDownTestCase()

	reqCreate := httptest.NewRequest(RandMethodRequest(), requestCreate, nil)
	reqApproxDocCount := httptest.NewRequest(RandMethodRequest(), fmt.Sprintf(requestApproxDocCount, collection), nil)
	wApproxDocCount := httptest.NewRecorder()
	wCreate := httptest.NewRecorder()

	var err error
	if HttpDB, err = db.OpenDB(tempDir); err != nil {
		panic(err)
	}

	Create(wCreate, reqCreate)
	ApproxDocCount(wApproxDocCount, reqApproxDocCount)

	if wApproxDocCount.Code != 200 || strings.TrimSpace(wApproxDocCount.Body.String()) != "0" {
		t.Error("Expected code 200 and count 0")
	}
}
