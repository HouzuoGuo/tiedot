package httpapi

import (
	"fmt"
	"github.com/HouzuoGuo/tiedot/db"
	"log"
	"net/http/httptest"
	"os"
	"testing"
)

var (
	// list api request
	requestCreteError = "http://localhost:8080/create"
	requestCreate     = fmt.Sprintf("http://localhost:8080/create?col=%s", collection)

	collection = "Feeds"
	tempDir    = "./tmp"
)

// setUp tests
func TestMain(m *testing.M) {
	if err := os.MkdirAll(tempDir, 0700); err != nil {
		log.Println(err)
	}
	retCode := m.Run()

	os.RemoveAll(tempDir)
	os.Exit(retCode)
}

func TestCreateError(t *testing.T) {
	req := httptest.NewRequest("GET", requestCreteError, nil)
	w := httptest.NewRecorder()
	Create(w, req)
	if w.Code != 400 && w.Body.String() == "Please pass POST/PUT/GET parameter value of 'col'" {
		t.Error("Expected return code 400 and error message")
	}
}

func TestCreateDuplicateCollection(t *testing.T) {
	req := httptest.NewRequest("GET", requestCreate, nil)
	w := httptest.NewRecorder()

	var err error
	if HttpDB, err = db.OpenDB(tempDir); err != nil {
		panic(err)
	}
	Create(w, req)
	Create(w, req)
	if w.Code != 400 && w.Body.String() != fmt.Sprintf("Collection %s already exists", collection) {
		t.Error("Expected code 400 if collection create duplicate")
	}
}
func TestCreate(t *testing.T) {
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
