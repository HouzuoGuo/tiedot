package httpapi

import (
	"bytes"
	"fmt"
	"github.com/HouzuoGuo/tiedot/db"
	"net/http/httptest"
	"strings"
	"testing"
)

var (
	requestInsert           = fmt.Sprintf("http://localhost:8080/insert?col=%s", collection)
	requestInsertWithoutCol = "http://localhost:8080/insert"
)

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
func TestInsertErrorUnmarshal(t *testing.T) {
	setupTestCase()
	defer tearDownTestCase()

	b := &bytes.Buffer{}
	b.WriteString("doc='{\"a\": 1, \"b\": 2}'")
	reqCreate := httptest.NewRequest("POST", requestCreate, nil)
	reqInsert := httptest.NewRequest("POST", requestInsert, b)
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
