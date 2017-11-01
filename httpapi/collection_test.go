package httpapi

import (
	"fmt"
	"github.com/HouzuoGuo/tiedot/db"
	"log"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
)

var (
	// list api request
	requestCreteError          = "http://localhost:8080/create"
	requestCreate              = fmt.Sprintf("http://localhost:8080/create?col=%s", collection)
	requestAll                 = "http://localhost:8080/all"
	requestRename              = fmt.Sprintf("http://localhost:8080/rename?old=%s&new=%s", collection, collectionNew)
	requestRenameMissingOld    = fmt.Sprintf("http://localhost:8080/rename?new=%s", collectionNew)
	requestRenameMissingNew    = fmt.Sprintf("http://localhost:8080/rename?old=%s", collection)
	requestDrop                = fmt.Sprintf("http://localhost:8080/drop?col=%s", collection)
	requestDropMissingParamCol = "http://localhost:8080/drop"
	requestScrubMissingColl    = "http://localhost:8080/scrub"
	requestScrub               = fmt.Sprintf("http://localhost:8080/scrub?col=%s", collection)
	requestSync                = "http://localhost:8080/sync"

	collection    = "Feeds"
	collectionNew = "Points"
	tempDir       = "./tmp"
)

// setUp and tearDown
func setupTestCase() {
	if err := os.MkdirAll(tempDir, 0700); err != nil {
		log.Println(err)
	}
}
func tearDownTestCase() {
	os.RemoveAll(tempDir)
}

// Test Create
func TestCreateError(t *testing.T) {
	setupTestCase()
	defer tearDownTestCase()
	req := httptest.NewRequest("GET", requestCreteError, nil)
	w := httptest.NewRecorder()
	Create(w, req)
	if w.Code != 400 || w.Body.String() == "Please pass POST/PUT/GET parameter value of 'col'" {
		t.Error("Expected return code 400 and error message")
	}
}
func TestCreateDuplicateCollection(t *testing.T) {
	setupTestCase()
	defer tearDownTestCase()
	req := httptest.NewRequest("GET", requestCreate, nil)
	w := httptest.NewRecorder()
	wDubl := httptest.NewRecorder()

	var err error
	if HttpDB, err = db.OpenDB(tempDir); err != nil {
		panic(err)
	}
	Create(w, req)
	Create(wDubl, req)

	if wDubl.Code != 400 || strings.TrimSpace(wDubl.Body.String()) != fmt.Sprintf("Collection %s already exists", collection) {
		t.Error("Expected code 400 if collection create duplicate")
	}
}

func TestCreate(t *testing.T) {
	setupTestCase()
	defer tearDownTestCase()
	req := httptest.NewRequest("GET", requestCreate, nil)
	w := httptest.NewRecorder()

	var err error
	if HttpDB, err = db.OpenDB(tempDir); err != nil {
		panic(err)
	}
	Create(w, req)
	if w.Code != 201 {
		t.Error("Expected code 201 after call method Create")
	}
}

// Test All
func TestAll(t *testing.T) {
	setupTestCase()
	defer tearDownTestCase()
	reqCreate := httptest.NewRequest("GET", requestCreate, nil)
	reqAll := httptest.NewRequest("GET", requestAll, nil)
	w := httptest.NewRecorder()

	var err error
	if HttpDB, err = db.OpenDB(tempDir); err != nil {
		panic(err)
	}
	Create(w, reqCreate)
	All(w, reqAll)
	if w.Code != 201 || w.Body.String() != fmt.Sprintf("[\"%s\"]", collection) {
		t.Error("Expected lists collection and status 201")
	}
}

// Test Rename
func TestRename(t *testing.T) {
	setupTestCase()
	defer tearDownTestCase()
	reqCreate := httptest.NewRequest("GET", requestCreate, nil)
	reqRename := httptest.NewRequest("GET", requestRename, nil)
	reqAll := httptest.NewRequest("GET", requestAll, nil)

	w := httptest.NewRecorder()
	wRename := httptest.NewRecorder()
	wAll := httptest.NewRecorder()

	var err error
	if HttpDB, err = db.OpenDB(tempDir); err != nil {
		panic(err)
	}
	Create(w, reqCreate)
	Rename(wRename, reqRename)
	All(wAll, reqAll)

	if wRename.Code != 200 || wAll.Body.String() != fmt.Sprintf("[\"%s\"]", collectionNew) {
		t.Error("Expected code 200 after rename and rename collection")
	}
}
func TestRenameMissingOldParameter(t *testing.T) {
	setupTestCase()
	defer tearDownTestCase()
	reqCreate := httptest.NewRequest("GET", requestCreate, nil)
	reqRename := httptest.NewRequest("GET", requestRenameMissingOld, nil)

	w := httptest.NewRecorder()
	wRename := httptest.NewRecorder()

	var err error
	if HttpDB, err = db.OpenDB(tempDir); err != nil {
		panic(err)
	}
	Create(w, reqCreate)
	Rename(wRename, reqRename)

	if wRename.Code != 400 || strings.TrimSpace(wRename.Body.String()) != "Please pass POST/PUT/GET parameter value of 'old'." {
		t.Error("Expected error code 400 and message missing parameter old")
	}
}
func TestRenameMissingNewParameter(t *testing.T) {
	setupTestCase()
	defer tearDownTestCase()
	reqCreate := httptest.NewRequest("GET", requestCreate, nil)
	reqRename := httptest.NewRequest("GET", requestRenameMissingNew, nil)

	w := httptest.NewRecorder()
	wRename := httptest.NewRecorder()

	var err error
	if HttpDB, err = db.OpenDB(tempDir); err != nil {
		panic(err)
	}
	Create(w, reqCreate)
	Rename(wRename, reqRename)

	if wRename.Code != 400 || strings.TrimSpace(wRename.Body.String()) != "Please pass POST/PUT/GET parameter value of 'new'." {
		t.Error("Expected error code 400 and message missing parameter new")
	}
}
func TestRenameError(t *testing.T) {
	setupTestCase()
	defer tearDownTestCase()
	reqRename := httptest.NewRequest("GET", requestRename, nil)
	wRename := httptest.NewRecorder()

	var err error
	if HttpDB, err = db.OpenDB(tempDir); err != nil {
		panic(err)
	}

	Rename(wRename, reqRename)
	if wRename.Code != 400 || strings.TrimSpace(wRename.Body.String()) != fmt.Sprintf("Collection %s does not exist", collection) {
		t.Error("Expected error code 400 and message missing collection")
	}
}

// Test Drop
func TestDrop(t *testing.T) {
	setupTestCase()
	defer tearDownTestCase()
	reqCreate := httptest.NewRequest("GET", requestCreate, nil)
	reqDrop := httptest.NewRequest("GET", requestDrop, nil)
	reqAll := httptest.NewRequest("GET", requestAll, nil)

	w := httptest.NewRecorder()
	wDrop := httptest.NewRecorder()
	wAll := httptest.NewRecorder()

	var err error
	if HttpDB, err = db.OpenDB(tempDir); err != nil {
		panic(err)
	}
	Create(w, reqCreate)
	Drop(wDrop, reqDrop)
	All(wAll, reqAll)

	if wDrop.Code != 200 || wAll.Body.String() != "[]" {
		t.Error("Expected code 200 and empty collection after call drop method")
	}
}
func TestDropMissingParameterCol(t *testing.T) {
	setupTestCase()
	defer tearDownTestCase()
	reqCreate := httptest.NewRequest("GET", requestCreate, nil)
	reqDrop := httptest.NewRequest("GET", requestDropMissingParamCol, nil)

	w := httptest.NewRecorder()
	wDrop := httptest.NewRecorder()

	var err error
	if HttpDB, err = db.OpenDB(tempDir); err != nil {
		panic(err)
	}
	Create(w, reqCreate)
	Drop(wDrop, reqDrop)

	if wDrop.Code != 400 || strings.TrimSpace(wDrop.Body.String()) != "Please pass POST/PUT/GET parameter value of 'col'." {
		t.Error("Expected code 400 and error message missing parameter 'col'")
	}
}
func TestDropMissingCol(t *testing.T) {
	setupTestCase()
	defer tearDownTestCase()
	reqDrop := httptest.NewRequest("GET", requestDrop, nil)
	wDrop := httptest.NewRecorder()

	var err error
	if HttpDB, err = db.OpenDB(tempDir); err != nil {
		panic(err)
	}

	Drop(wDrop, reqDrop)

	if wDrop.Code != 400 || strings.TrimSpace(wDrop.Body.String()) != fmt.Sprintf("Collection %s does not exist", collection) {
		t.Error("Expected code 400 and error message missing collecion")
	}
}

// Test Scrub
func TestScrubMissingCollectParam(t *testing.T) {
	setupTestCase()
	defer tearDownTestCase()
	req := httptest.NewRequest("GET", requestScrubMissingColl, nil)
	w := httptest.NewRecorder()

	var err error
	if HttpDB, err = db.OpenDB(tempDir); err != nil {
		panic(err)
	}

	Scrub(w, req)

	if w.Code != 400 || strings.TrimSpace(w.Body.String()) != "Please pass POST/PUT/GET parameter value of 'col'." {
		t.Error("Expected code 400 and error message missing collecion")
	}
}
func TestScrubCollectionNotExist(t *testing.T) {
	setupTestCase()
	defer tearDownTestCase()
	req := httptest.NewRequest("GET", requestScrub, nil)
	w := httptest.NewRecorder()

	var err error
	if HttpDB, err = db.OpenDB(tempDir); err != nil {
		panic(err)
	}
	Scrub(w, req)
	if w.Code != 400 || strings.TrimSpace(w.Body.String()) != fmt.Sprintf("Collection %s does not exist", collection) {
		t.Error("Expected code 400 and error message collecion not exist")
	}
}
func TestScrub(t *testing.T) {
	setupTestCase()
	defer tearDownTestCase()
	reqCreate := httptest.NewRequest("GET", requestCreate, nil)
	reqScrub := httptest.NewRequest("GET", requestScrub, nil)
	wCreate := httptest.NewRecorder()
	w := httptest.NewRecorder()

	var err error
	if HttpDB, err = db.OpenDB(tempDir); err != nil {
		panic(err)
	}

	Create(wCreate, reqCreate)
	Scrub(w, reqScrub)

	if w.Code != 200 {
		t.Error("Expected code 200 after call scrub")
	}
}

// Test Sync
func TestSync(t *testing.T) {
	rSync := httptest.NewRequest("GET", requestSync, nil)
	w := httptest.NewRecorder()
	Sync(w, rSync)
	if w.Code != 200 || w.HeaderMap["Content-Type"][0] != "text/plain" || w.HeaderMap["Cache-Control"][0] != "must-revalidate" {
		t.Error("Expected code 200 and Content-Type: text/plain and Cache-Control : must-revalidate")
	}
}