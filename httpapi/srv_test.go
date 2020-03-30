package httpapi

import (
	"fmt"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/HouzuoGuo/tiedot/db"
)

var (
	requestWelcome    = "http://localhost:8080/"
	requestWelcomeErr = "http://localhost:8080/%s"
)

func TestSrc(t *testing.T) {
	testsSrc := []func(t *testing.T){
		TWelcome,
		TRequireFalse,
		TRequireTrue,
		TWelcomeError,
	}
	managerSubTests(testsSrc, "src_test", t)
}
func TWelcome(t *testing.T) {
	setupTestCase()
	defer tearDownTestCase()

	req := httptest.NewRequest(RandMethodRequest(), requestWelcome, nil)
	w := httptest.NewRecorder()

	var err error
	if HttpDB, err = db.OpenDB(tempDir); err != nil {
		panic(err)
	}
	Welcome(w, req)
	if w.Code != 200 || strings.TrimSpace(w.Body.String()) != "Welcome to tiedot" {
		t.Error("Expected code 200 and welcome message.")
	}
}
func TWelcomeError(t *testing.T) {
	setupTestCase()
	defer tearDownTestCase()

	req := httptest.NewRequest(RandMethodRequest(), fmt.Sprintf(requestWelcomeErr, "test"), nil)
	w := httptest.NewRecorder()

	var err error
	if HttpDB, err = db.OpenDB(tempDir); err != nil {
		panic(err)
	}
	Welcome(w, req)
	if w.Code != 404 || strings.TrimSpace(w.Body.String()) != "Invalid API endpoint" {
		t.Error("Expected code 404 and error message api endpoint.")
	}
}
func TRequireFalse(t *testing.T) {
	setupTestCase()
	defer tearDownTestCase()

	req := httptest.NewRequest(RandMethodRequest(), requestWelcome, nil)
	w := httptest.NewRecorder()

	var err error
	var test string
	if HttpDB, err = db.OpenDB(tempDir); err != nil {
		panic(err)
	}
	Require(w, req, "test", &test)

	if w.Code != 400 || strings.TrimSpace(w.Body.String()) != "Please pass POST/PUT/GET parameter value of 'test'." {
		t.Error("Expected code 400 and error message.")
	}
}
func TRequireTrue(t *testing.T) {
	setupTestCase()
	defer tearDownTestCase()

	req := httptest.NewRequest(RandMethodRequest(), requestWelcome, nil)
	w := httptest.NewRecorder()

	var err error
	var test string
	if HttpDB, err = db.OpenDB(tempDir); err != nil {
		panic(err)
	}
	val := url.Values{}
	val.Set("test", "test")
	req.Form = val
	if Require(w, req, "test", &test) != true {
		t.Error("Expected bool true from require function")
	}
}
