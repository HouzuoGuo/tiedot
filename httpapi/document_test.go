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
	requestGetPageNotPage  = "http://localhost:8080/getpage?col=%s"
	requestGetPageNotTotal = "http://localhost:8080/getpage?col=%s&page=%s"
	requestGetPage         = "http://localhost:8080/getpage?col=%s&page=%s&total=%d"

	requestUpdateNotCol = "http://localhost:8080/update"
	requestUpdateNotId  = "http://localhost:8080/update?col=%s"
	requestUpdateNotDoc = "http://localhost:8080/update?col=%s&id=%s"
	requestUpdate       = "http://localhost:8080/update?col=%s&id=%s"

	requestDeleteNotCol = "http://localhost:8080/delete"
	requestDeleteNotId  = "http://localhost:8080/delete?col=%s"
	requestDelete	    = "http://localhost:8080/delete?col=%s&id=%s"

	page  = "1"
	total = 2
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

	reqGet := httptest.NewRequest("POST", fmt.Sprintf(requestGet, collection, strings.TrimSpace(wInsert.Body.String())), nil)

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

	wGet := httptest.NewRecorder()

	randIntStr := RandStringBytes(5)
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
	reqGet := httptest.NewRequest("POST", fmt.Sprintf(requestGetNotCol, randIntStr), nil)
	wGet := httptest.NewRecorder()

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
	reqGet := httptest.NewRequest("POST", fmt.Sprintf(requestGetNotId, collection), nil)
	wGet := httptest.NewRecorder()
	Get(wGet, reqGet)

	if wGet.Code != 400 || strings.TrimSpace(wGet.Body.String()) != "Please pass POST/PUT/GET parameter value of 'id'." {
		t.Error("Expected code 400 and message error not such param 'id'")
	}
}

// Test GetPage
func TestGetPageNotCol(t *testing.T) {
	setupTestCase()
	defer tearDownTestCase()

	reqGetPage := httptest.NewRequest("POST", requestGetPageNotCol, nil)
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
func TestGetPageNotPage(t *testing.T) {
	setupTestCase()
	defer tearDownTestCase()

	reqGetPage := httptest.NewRequest("POST", fmt.Sprintf(requestGetPageNotPage, collection), nil)
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
func TestGetPageNotTotal(t *testing.T) {
	setupTestCase()
	defer tearDownTestCase()

	reqGetPage := httptest.NewRequest("POST", fmt.Sprintf(requestGetPageNotTotal, collection, page), nil)
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
func TestGetPageErrorTotal(t *testing.T) {
	setupTestCase()
	defer tearDownTestCase()

	reqGetPage := httptest.NewRequest("POST", fmt.Sprintf(requestGetPage, collection, page, 0), nil)
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
func TestGetPageErrorPage(t *testing.T) {
	setupTestCase()
	defer tearDownTestCase()

	reqGetPage := httptest.NewRequest("POST", fmt.Sprintf(requestGetPage, collection, "-1", total), nil)
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
func TestGetPageCollectionNotExist(t *testing.T) {
	setupTestCase()
	defer tearDownTestCase()

	b := &bytes.Buffer{}
	b.WriteString("{\"a\": 1, \"b\": 2}")

	reqGetPage := httptest.NewRequest("GET", fmt.Sprintf(requestGetPage, collection, page, total), nil)
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
func TestGetPage(t *testing.T) {
	setupTestCase()
	defer tearDownTestCase()

	b := &bytes.Buffer{}
	b.WriteString("{\"a\": 1, \"b\": 2}")

	reqCreate := httptest.NewRequest("GET", requestCreate, nil)
	reqInsert := httptest.NewRequest("GET", requestInsertWithoutDoc, b)
	reqGetPage := httptest.NewRequest("GET", fmt.Sprintf(requestGetPage, collection, page, total), nil)

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

// Test Update
func TestUpdateNotCol(t *testing.T) {
	setupTestCase()
	defer tearDownTestCase()

	reqUpdate := httptest.NewRequest("POST", requestUpdateNotCol, nil)
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
func TestUpdateNotId(t *testing.T) {
	setupTestCase()
	defer tearDownTestCase()

	reqUpdate := httptest.NewRequest("POST", fmt.Sprintf(requestUpdateNotId, collection), nil)
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
func TestUpdateNotDoc(t *testing.T) {
	setupTestCase()
	defer tearDownTestCase()

	wUpdate := httptest.NewRecorder()

	var err error
	if HttpDB, err = db.OpenDB(tempDir); err != nil {
		panic(err)
	}

	reqUpdate := httptest.NewRequest("POST", fmt.Sprintf(requestUpdateNotDoc, collection, "1"), nil)
	Update(wUpdate, reqUpdate)

	if wUpdate.Code != 400 || strings.TrimSpace(wUpdate.Body.String()) != "Please pass POST/PUT/GET parameter value of 'doc'." {
		t.Error("Expected code 400 and message error value of 'doc'")
	}
}
func TestUpdateInvalidId(t *testing.T) {
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
	reqUpdate := httptest.NewRequest("POST", fmt.Sprintf(requestUpdate, collection, randId), b)
	Update(wUpdate, reqUpdate)

	if wUpdate.Code != 400 || strings.TrimSpace(wUpdate.Body.String()) != fmt.Sprintf("Invalid document ID '%s'.", randId) {
		t.Error("Expected code 400 and message error invalid document id.")
	}
}
func TestUpdateJsonError(t *testing.T) {
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

	reqUpdate := httptest.NewRequest("POST", fmt.Sprintf(requestUpdate, collection, "1"), b)
	Update(wUpdate, reqUpdate)

	if wUpdate.Code != 400 || strings.TrimSpace(wUpdate.Body.String()) != "'map[]' is not valid JSON document." {
		t.Error("Expected code 400 and message error is not valid json")
	}
}
func TestUpdateCollectionNotExist(t *testing.T) {
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

	reqUpdate := httptest.NewRequest("POST", fmt.Sprintf(requestUpdate, collection, "1"), b)
	Update(wUpdate, reqUpdate)

	if wUpdate.Code != 400 || strings.TrimSpace(wUpdate.Body.String()) != fmt.Sprintf("Collection '%s' does not exist.", collection) {
		t.Error("Expected code 400 and message error collection is not exist")
	}
}
func TestUpdate(t *testing.T) {
	setupTestCase()
	defer tearDownTestCase()
	jsonStr := "{\"a\":1,\"b\":2}"
	jsonStrForUpdate := "{\"a\":1,\"b\":3}"

	b := &bytes.Buffer{}
	b.WriteString(jsonStr)

	b2 := &bytes.Buffer{}
	b2.WriteString(jsonStrForUpdate)

	reqCreate := httptest.NewRequest("GET", requestCreate, nil)
	reqInsert := httptest.NewRequest("GET", requestInsertWithoutDoc, b)

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

	reqUpdate := httptest.NewRequest("POST", fmt.Sprintf(requestUpdate, collection, strings.TrimSpace(wInsert.Body.String())), b2)
	Update(wUpdate, reqUpdate)

	reqGet := httptest.NewRequest("POST", fmt.Sprintf(requestGet, collection, strings.TrimSpace(wInsert.Body.String())), b)
	Get(wGet, reqGet)

	if wUpdate.Code != 200 || wGet.Code != 200 || strings.TrimSpace(wGet.Body.String()) != "{\"a\":1,\"b\":3}" {
		t.Error("Expected code 200 and get update document")
	}
}
func TestUpdateError(t *testing.T) {
	setupTestCase()
	defer tearDownTestCase()

	jsonStrForUpdate := "{\"a\":1,\"b\":3}"

	b2 := &bytes.Buffer{}
	b2.WriteString(jsonStrForUpdate)

	reqCreate := httptest.NewRequest("GET", requestCreate, nil)
	wCreate := httptest.NewRecorder()
	wUpdate := httptest.NewRecorder()

	var err error
	if HttpDB, err = db.OpenDB(tempDir); err != nil {
		panic(err)
	}

	Create(wCreate, reqCreate)

	reqUpdate := httptest.NewRequest("POST", fmt.Sprintf(requestUpdate, collection, "2"), b2)
	Update(wUpdate, reqUpdate)

	if wUpdate.Code != 500 || strings.TrimSpace(wUpdate.Body.String()) != "Document `2` does not exist" {
		t.Error("Expected code 500 and message error document not exist")
	}
}

//Test Delete
func TestDeleteNotCol(t *testing.T) {
	setupTestCase()
	defer tearDownTestCase()

	reqDelete := httptest.NewRequest("GET", requestDeleteNotCol, nil)
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
func TestDeleteNotId(t *testing.T) {
	setupTestCase()
	defer tearDownTestCase()

	reqDelete := httptest.NewRequest("GET", fmt.Sprintf(requestDeleteNotId, collection), nil)
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
func TestDeleteInvalidId(t *testing.T) {
	setupTestCase()
	defer tearDownTestCase()
	randId := RandStringBytes(5)
	reqDelete := httptest.NewRequest("GET", fmt.Sprintf(requestDelete, collection, randId), nil)
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
func TestDeleteCollectionNotExist(t *testing.T) {
	setupTestCase()
	defer tearDownTestCase()

	reqDelete := httptest.NewRequest("GET", fmt.Sprintf(requestDelete, collection, "1"), nil)
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
func TestDelete(t *testing.T) {
	setupTestCase()
	defer tearDownTestCase()
	b := &bytes.Buffer{}
	jsonStr := "{\"a\":1,\"b\":2}"
	b.WriteString(jsonStr)

	reqCreate := httptest.NewRequest("GET", requestCreate, nil)
	reqInsert := httptest.NewRequest("GET", requestInsertWithoutDoc, b)

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
	reqDelete := httptest.NewRequest("GET", fmt.Sprintf(requestDelete, collection, idRecord), nil)
	reqGet := httptest.NewRequest("POST", fmt.Sprintf(requestGet, collection, strings.TrimSpace(wInsert.Body.String())), nil)
	Delete(wDelete, reqDelete)
	Get(wGet, reqGet)

	if wDelete.Code != 200 || wGet.Code != 404 || strings.TrimSpace(wGet.Body.String()) != fmt.Sprintf("No such document ID %s.", idRecord) {
		t.Error("Expected code 200 and after delete message error not such document with the specified 'id'")
	}
}