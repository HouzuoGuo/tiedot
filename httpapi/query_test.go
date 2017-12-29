package httpapi

import (
	"encoding/json"
	"fmt"
	"github.com/HouzuoGuo/tiedot/db"
	"github.com/bouk/monkey"
	"github.com/pkg/errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
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
func TestQuery(t *testing.T) {
	setupTestCase()
	defer tearDownTestCase()

	path := monkey.Patch(db.EvalQuery, func(q interface{}, src *db.Col, result *map[int]struct{}) (err error) {
		*result = map[int]struct{}{2: struct{}{}}
		return nil
	})
	defer path.Unpatch()
	var col *db.Col
	pathCol := monkey.PatchInstanceMethod(reflect.TypeOf(col), "Read", func(_ *db.Col, id int) (doc map[string]interface{}, err error) {
		return map[string]interface{}{"34234234": struct{}{}}, nil
	})
	defer pathCol.Unpatch()

	reqCreate := httptest.NewRequest(RandMethodRequest(), requestCreate, nil)
	req := httptest.NewRequest("POST", fmt.Sprintf(requestQueryWithAll, collection, "1"), nil)
	w := httptest.NewRecorder()
	wQuery := httptest.NewRecorder()
	var err error
	if HttpDB, err = db.OpenDB(tempDir); err != nil {
		panic(err)
	}
	Create(w, reqCreate)
	Query(wQuery, req)
	if wQuery.Code != http.StatusOK {
		t.Errorf("Expected status %d", http.StatusOK)
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
func TestQueryErrEvalQuery(t *testing.T) {
	errMessage := "Error eval query"
	path := monkey.Patch(db.EvalQuery, func(q interface{}, src *db.Col, result *map[int]struct{}) (err error) {
		return errors.New(errMessage)
	})
	defer path.Unpatch()
	setupTestCase()
	defer tearDownTestCase()

	reqCreate := httptest.NewRequest(RandMethodRequest(), requestCreate, nil)
	req := httptest.NewRequest(RandMethodRequest(), fmt.Sprintf(requestQueryWithAll, collection, "1"), nil)
	w := httptest.NewRecorder()
	wQuery := httptest.NewRecorder()
	var err error
	if HttpDB, err = db.OpenDB(tempDir); err != nil {
		panic(err)
	}
	Create(w, reqCreate)
	Query(wQuery, req)

	if wQuery.Code != http.StatusBadRequest || strings.TrimSpace(wQuery.Body.String()) != errMessage {
		t.Errorf("Expected status %d and error message eval query", http.StatusBadRequest)
	}
}
func TestQuerySerializeError(t *testing.T) {
	setupTestCase()
	defer tearDownTestCase()

	reqCreate := httptest.NewRequest(RandMethodRequest(), requestCreate, nil)
	req := httptest.NewRequest(RandMethodRequest(), fmt.Sprintf(requestQueryWithAll, collection, "1"), nil)
	w := httptest.NewRecorder()
	wQuery := httptest.NewRecorder()
	var err error
	if HttpDB, err = db.OpenDB(tempDir); err != nil {
		panic(err)
	}

	path := monkey.Patch(json.Marshal, func(v interface{}) ([]byte, error) {
		return nil, errors.New("error marshal")
	})
	defer path.Unpatch()
	Create(w, reqCreate)
	Query(wQuery, req)
	if wQuery.Code != http.StatusInternalServerError || strings.TrimSpace(wQuery.Body.String()) != "Server error: query returned invalid structure" {
		t.Errorf("Expected status %d and error message invalid structure", http.StatusInternalServerError)
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
func TestCountErrEvalQuery(t *testing.T) {
	setupTestCase()
	defer tearDownTestCase()
	errMessage := "Error eval query"
	path := monkey.Patch(db.EvalQuery, func(q interface{}, src *db.Col, result *map[int]struct{}) (err error) {
		return errors.New(errMessage)
	})
	defer path.Unpatch()
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
	if wCount.Code != http.StatusBadRequest || strings.TrimSpace(wCount.Body.String()) != errMessage {
		t.Errorf("Expected status %d and error message eval query", http.StatusBadRequest)
	}
}
