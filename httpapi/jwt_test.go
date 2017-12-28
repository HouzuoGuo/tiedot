package httpapi

import (
	"bytes"
	"fmt"
	"github.com/HouzuoGuo/tiedot/db"
	"github.com/bouk/monkey"
	jwt "github.com/dgrijalva/jwt-go"
	"github.com/dgrijalva/jwt-go/request"
	"github.com/pkg/errors"
	"io/ioutil"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"
)

var (
	urlJwt          = "http://localhost:8080/"
	jwtUrlAuth      = "http://localhost:8080/getjwt?user=%s&pass=%s"
	jwtUrlWithToken = "http://localhost:8080/getjwt?access_token=%s"
)

func TestJWTToken(t *testing.T) {
	var err error
	var privateKeyContent, publicKeyContent []byte
	if privateKeyContent, err = ioutil.ReadFile("jwt-test.key"); err != nil {
		t.Fatal(err)
	}
	if publicKeyContent, err = ioutil.ReadFile("jwt-test.pub"); err != nil {
		t.Fatal(err)
	}
	if privateKey, err = jwt.ParseRSAPrivateKeyFromPEM(privateKeyContent); err != nil {
		t.Fatal(err)
	}
	if publicKey, err = jwt.ParseRSAPublicKeyFromPEM(publicKeyContent); err != nil {
		t.Fatal(err)
	}
	token := jwt.New(jwt.GetSigningMethod("RS256"))
	token.Claims = jwt.MapClaims{
		"PERMISSION": "admin@tiedot",
		"exp":        time.Now().Add(time.Hour * 72).Unix(),
	}
	ts, err := token.SignedString(privateKey)
	if err != nil {
		t.Fatal(err)
	}
	if token, err = jwt.Parse(ts, func(ts *jwt.Token) (interface{}, error) {
		return publicKey, nil
	}); err != nil {
		t.Fatal(err)
	}
	if token.Valid {
		t.Log(token)
	} else {
		t.Log(token)
		t.Fail()
	}
}
func TestJwtInitSetupErrorCreateError(t *testing.T) {
	var (
		database *db.DB
		str      bytes.Buffer
	)

	defer tearDownTestCase()
	defer func() {
		r := recover()
		if r == nil || !strings.Contains(str.String(), "JWT: failed to create JWT identity collection") {
			t.Errorf("Code not panic")
		}
	}()

	log.SetOutput(&str)
	errMessage := "Error create collection"
	path := monkey.PatchInstanceMethod(reflect.TypeOf(database), "Create", func(_ *db.DB, name string) error {
		return errors.New(errMessage)
	})
	defer path.Unpatch()
	var err error
	if HttpDB, err = db.OpenDB(tempDir); err != nil {
		panic(err)
	}
	jwtInitSetup()
}
func TestJwtInitSetup(t *testing.T) {
	var err error
	defer tearDownTestCase()
	if HttpDB, err = db.OpenDB(tempDir); err != nil {
		panic(err)
	}
	HttpDB.Create(JWT_COL_NAME)
	jwtCol := HttpDB.Use(JWT_COL_NAME)
	jwtCol.Index([]string{"index"})
	jwtInitSetup()
	if _, err := os.Stat(tempDir); err != nil {
		t.Error("Expected folder jwt not exist Error:" + err.Error())
	}
}
func TestJwtInitSetupErrorInsert(t *testing.T) {
	var err error
	defer tearDownTestCase()
	var str bytes.Buffer
	errMessage := "Error when insert in collection"
	log.SetOutput(&str)
	defer func() {
		if r := recover(); r == nil || !strings.Contains(str.String(), fmt.Sprintf("JWT: failed to create default admin user - %v", errMessage)) {
			t.Errorf("Code not panic")
		}
	}()
	if HttpDB, err = db.OpenDB(tempDir); err != nil {
		panic(err)
	}

	var database *db.Col
	path := monkey.PatchInstanceMethod(reflect.TypeOf(database), "Insert", func(_ *db.Col, doc map[string]interface{}) (id int, err error) {
		return 0, errors.New(errMessage)
	})
	defer path.Unpatch()
	jwtInitSetup()
}
func TestJwtInitSetupEvalQueryErr(t *testing.T) {
	var err error
	defer tearDownTestCase()
	var str bytes.Buffer
	errMessage := "Error EvalQuery"
	log.SetOutput(&str)
	defer func() {
		if r := recover(); r == nil || !strings.Contains(str.String(), fmt.Sprintf("JWT: failed to query admin user ID - %v", errMessage)) {
			t.Errorf("Code not panic")
		}
	}()
	if HttpDB, err = db.OpenDB(tempDir); err != nil {
		panic(err)
	}

	path := monkey.Patch(db.EvalQuery, func(q interface{}, src *db.Col, result *map[int]struct{}) (err error) {
		return errors.New(errMessage)
	})
	defer path.Unpatch()
	jwtInitSetup()
}
func TestJwtInitSetupSetIndex(t *testing.T) {
	var (
		err error
		str bytes.Buffer
		col *db.Col
	)
	defer tearDownTestCase()

	errMessage := "Insert index in collection"
	log.SetOutput(&str)
	defer func() {
		if r := recover(); r == nil || !strings.Contains(str.String(), fmt.Sprintf("JWT: failed to create collection index - %v", errMessage)) {
			t.Errorf("Code not panic")
		}
	}()

	if HttpDB, err = db.OpenDB(tempDir); err != nil {
		panic(err)
	}
	HttpDB.Create(JWT_COL_NAME)
	jwtCol := HttpDB.Use(JWT_COL_NAME)
	jwtCol.Index([]string{strings.Join([]string{JWT_PASS_ATTR}, db.INDEX_PATH_SEP)})

	path := monkey.PatchInstanceMethod(reflect.TypeOf(col), "Index", func(_ *db.Col, idxPath []string) (err error) {
		return errors.New(errMessage)
	})
	defer path.Unpatch()
	jwtInitSetup()
}
func TestAddCommonJwtRespHeadersSetOrigin(t *testing.T) {
	req := httptest.NewRequest("GET", urlJwt, nil)
	req.Header.Set("Origin", "test")

	w := httptest.NewRecorder()
	addCommonJwtRespHeaders(w, req)
	if w.HeaderMap.Get("Access-Control-Allow-Origin") != "test" ||
		w.HeaderMap.Get("Cache-Control") != "must-revalidate" ||
		w.HeaderMap.Get("Access-Control-Expose-Headers") != "Authorization" ||
		w.HeaderMap.Get("Access-Control-Allow-Headers") != "Content-Type, Content-Length, Accept-Encoding, X-CSRF-Token, Authorization" {
		t.Error("Expected valid headers origin")
	}
}
func TestGetJWTNotUserParameter(t *testing.T) {
	defer tearDownTestCase()
	req := httptest.NewRequest("GET", urlJwt, nil)
	req.Header.Set("Origin", "test")

	w := httptest.NewRecorder()
	getJWT(w, req)
	if w.Code != http.StatusBadRequest || strings.TrimSpace(w.Body.String()) != "Please pass JWT 'user' parameter" {
		t.Errorf("Expeceted code %d and error message.", http.StatusBadRequest)
	}
}
func TestGetJWTNotCollection(t *testing.T) {
	defer tearDownTestCase()
	req := httptest.NewRequest("GET", fmt.Sprintf(jwtUrlAuth, "test", ""), nil)
	req.Header.Set("Origin", "test")
	w := httptest.NewRecorder()
	var err error
	if HttpDB, err = db.OpenDB(tempDir); err != nil {
		panic(err)
	}
	getJWT(w, req)

	if w.Code != http.StatusInternalServerError || strings.TrimSpace(w.Body.String()) != "Server is missing JWT identity collection, please restart the server." {
		t.Errorf("Expeceted code %d and error message : server is missing JWT.", http.StatusInternalServerError)
	}
}
func TestGetJWTQueryFailed(t *testing.T) {
	defer tearDownTestCase()
	req := httptest.NewRequest("GET", fmt.Sprintf(jwtUrlAuth, "test", "12345"), nil)
	w := httptest.NewRecorder()
	var (
		err error
		str bytes.Buffer
	)
	log.SetOutput(&str)

	if HttpDB, err = db.OpenDB(tempDir); err != nil {
		panic(err)
	}
	HttpDB.Create(JWT_COL_NAME)
	getJWT(w, req)
	if w.Code != http.StatusInternalServerError ||
		!strings.Contains(w.Body.String(), "Query failed in JWT identity collection") ||
		!strings.Contains(str.String(), "Query failed in JWT identity collection") {
		t.Errorf("Expeceted code %d and error message : Query failed.", http.StatusInternalServerError)
	}
}
func TestGetJWTPasswordError(t *testing.T) {
	defer tearDownTestCase()
	req := httptest.NewRequest("GET", fmt.Sprintf(jwtUrlAuth, JWT_USER_ADMIN, "12345"), nil)
	w := httptest.NewRecorder()
	var (
		err error
		str bytes.Buffer
	)
	log.SetOutput(&str)

	if HttpDB, err = db.OpenDB(tempDir); err != nil {
		panic(err)
	}

	jwtInitSetup()
	getJWT(w, req)

	if w.Code != http.StatusUnauthorized || strings.TrimSpace(w.Body.String()) != "Invalid password" || !strings.Contains(str.String(), "JWT: successfully initialized DB for JWT features. The default user 'admin' has been created.") {
		t.Error("Expected StatusUnauthorized and error message jwt verification")
	}
}
func TestGetJWT(t *testing.T) {
	defer tearDownTestCase()
	req := httptest.NewRequest("GET", fmt.Sprintf(jwtUrlAuth, JWT_USER_ADMIN, ""), nil)
	w := httptest.NewRecorder()
	var (
		err error
		str bytes.Buffer
	)
	log.SetOutput(&str)

	if HttpDB, err = db.OpenDB(tempDir); err != nil {
		panic(err)
	}
	var privateKeyContent []byte
	if privateKeyContent, err = ioutil.ReadFile("jwt-test.key"); err != nil {
		t.Fatal(err)
	}
	if privateKey, err = jwt.ParseRSAPrivateKeyFromPEM(privateKeyContent); err != nil {
		t.Fatal(err)
	}

	jwtInitSetup()
	getJWT(w, req)
	if w.Code != http.StatusOK {
		t.Error("Expected StatusOKn")
	}
}
func TestGetReadError(t *testing.T) {
	defer tearDownTestCase()
	req := httptest.NewRequest("GET", fmt.Sprintf(jwtUrlAuth, JWT_USER_ADMIN, ""), nil)
	w := httptest.NewRecorder()
	var (
		err      error
		str      bytes.Buffer
		database *db.Col
	)
	log.SetOutput(&str)

	path := monkey.PatchInstanceMethod(reflect.TypeOf(database), "Read", func(_ *db.Col, id int) (doc map[string]interface{}, err error) {
		return nil, errors.New("Error Read from collection")
	})
	defer path.Unpatch()

	if HttpDB, err = db.OpenDB(tempDir); err != nil {
		panic(err)
	}

	jwtInitSetup()
	getJWT(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Error("Expected status unauthirized")
	}
}
func TestExtractTokenErr(t *testing.T) {
	req := httptest.NewRequest("GET", fmt.Sprintf(jwtUrlAuth, JWT_USER_ADMIN, ""), nil)
	token := TokenExtractor{}
	var err error
	_, err = token.ExtractToken(req)
	fmt.Println()
	if err.Error() != request.ErrNoTokenInRequest.Error() {
		t.Error("Expected error message no token")
	}
}
func TestExtractToken(t *testing.T) {
	tokenChar := "2lm52p3m35p2m3"
	req := httptest.NewRequest("GET", fmt.Sprintf(jwtUrlWithToken, tokenChar), nil)
	token := TokenExtractor{}
	//var err error
	r, _ := token.ExtractToken(req)

	if r != tokenChar {
		t.Error("Expected token")
	}
}
func TestCheckJWTErrorJwt(t *testing.T) {
	req := httptest.NewRequest("GET", urlJwt, nil)
	w := httptest.NewRecorder()
	checkJWT(w, req)

	if w.Code != http.StatusUnauthorized || strings.TrimSpace(w.Body.String()) != `{"error": "JWT not valid, no token present in request"}` {
		t.Error("Expected error jwt not valid")
	}
}
func TestCheckJWT(t *testing.T) {
	var err error
	var privateKeyContent, publicKeyContent []byte
	if privateKeyContent, err = ioutil.ReadFile("jwt-test.key"); err != nil {
		t.Fatal(err)
	}
	if publicKeyContent, err = ioutil.ReadFile("jwt-test.pub"); err != nil {
		t.Fatal(err)
	}
	if privateKey, err = jwt.ParseRSAPrivateKeyFromPEM(privateKeyContent); err != nil {
		t.Fatal(err)
	}
	if publicKey, err = jwt.ParseRSAPublicKeyFromPEM(publicKeyContent); err != nil {
		t.Fatal(err)
	}
	token := jwt.New(jwt.GetSigningMethod("RS256"))
	ts, err := token.SignedString(privateKey)

	req := httptest.NewRequest("GET", fmt.Sprintf(jwtUrlWithToken, ts), nil)
	w := httptest.NewRecorder()
	checkJWT(w, req)

	if w.Code != http.StatusOK {
		t.Error("Expected status 200 after check token")
	}
}
func TestCheckJWTMethodErrorAccessType(t *testing.T) {
	var err error
	var privateKeyContent, publicKeyContent []byte
	if privateKeyContent, err = ioutil.ReadFile("jwt-test.key"); err != nil {
		t.Fatal(err)
	}
	if publicKeyContent, err = ioutil.ReadFile("jwt-test.pub"); err != nil {
		t.Fatal(err)
	}
	if privateKey, err = jwt.ParseRSAPrivateKeyFromPEM(privateKeyContent); err != nil {
		t.Fatal(err)
	}
	if publicKey, err = jwt.ParseRSAPublicKeyFromPEM(publicKeyContent); err != nil {
		t.Fatal(err)
	}
	token := jwt.New(jwt.GetSigningMethod("PS256"))
	ts, err := token.SignedString(privateKey)

	req := httptest.NewRequest("GET", fmt.Sprintf(jwtUrlWithToken, ts), nil)
	w := httptest.NewRecorder()
	checkJWT(w, req)

	if w.Code != http.StatusUnauthorized || strings.TrimSpace(w.Body.String()) != `{"error": "JWT not valid, Unexpected signing method: PS256"}` {
		t.Error("Expected status 401 message error method")
	}
}
func TestJwtWrapStatusUnauthorized(t *testing.T) {
	req := httptest.NewRequest("GET", urlJwt, nil)
	w := httptest.NewRecorder()
	jwtWrap(func(w http.ResponseWriter, r *http.Request) {

	})(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Error("Expected status StatusUnauthorized")
	}
}
func TestJwtWrapMethodNotRsa(t *testing.T) {
	var err error
	var privateKeyContent, publicKeyContent []byte
	if privateKeyContent, err = ioutil.ReadFile("jwt-test.key"); err != nil {
		t.Fatal(err)
	}
	if publicKeyContent, err = ioutil.ReadFile("jwt-test.pub"); err != nil {
		t.Fatal(err)
	}
	if privateKey, err = jwt.ParseRSAPrivateKeyFromPEM(privateKeyContent); err != nil {
		t.Fatal(err)
	}
	if publicKey, err = jwt.ParseRSAPublicKeyFromPEM(publicKeyContent); err != nil {
		t.Fatal(err)
	}
	token := jwt.New(jwt.GetSigningMethod("PS256"))

	token.Claims = jwt.MapClaims{
		"PERMISSION":       "admin@tiedot",
		JWT_ENDPOINTS_ATTR: []interface{}{},
		"exp":              time.Now().Add(time.Hour * 72).Unix(),
	}
	ts, err := token.SignedString(privateKey)

	req := httptest.NewRequest("GET", fmt.Sprintf(jwtUrlWithToken, ts), nil)
	w := httptest.NewRecorder()
	jwtWrap(func(w http.ResponseWriter, r *http.Request) {

	})(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Error("Expected status 401")
	}
}
func TestJwtWrapMethodAdmin(t *testing.T) {
	var err error
	var privateKeyContent, publicKeyContent []byte
	if privateKeyContent, err = ioutil.ReadFile("jwt-test.key"); err != nil {
		t.Fatal(err)
	}
	if publicKeyContent, err = ioutil.ReadFile("jwt-test.pub"); err != nil {
		t.Fatal(err)
	}
	if privateKey, err = jwt.ParseRSAPrivateKeyFromPEM(privateKeyContent); err != nil {
		t.Fatal(err)
	}
	if publicKey, err = jwt.ParseRSAPublicKeyFromPEM(publicKeyContent); err != nil {
		t.Fatal(err)
	}
	token := jwt.New(jwt.GetSigningMethod("RS256"))

	token.Claims = jwt.MapClaims{
		JWT_USER_ATTR: JWT_USER_ADMIN,
		"PERMISSION":  "admin@tiedot",
		"exp":         time.Now().Add(time.Hour * 72).Unix(),
	}
	ts, err := token.SignedString(privateKey)

	req := httptest.NewRequest("GET", fmt.Sprintf(jwtUrlWithToken, ts), nil)
	w := httptest.NewRecorder()
	jwtWrap(func(w http.ResponseWriter, r *http.Request) {

	})(w, req)

	if w.Code != http.StatusOK {
		t.Error("Expected status 200")
	}
}
func TestJwtWrapNotSliceEndPoints(t *testing.T) {
	var err error
	var privateKeyContent, publicKeyContent []byte
	if privateKeyContent, err = ioutil.ReadFile("jwt-test.key"); err != nil {
		t.Fatal(err)
	}
	if publicKeyContent, err = ioutil.ReadFile("jwt-test.pub"); err != nil {
		t.Fatal(err)
	}
	if privateKey, err = jwt.ParseRSAPrivateKeyFromPEM(privateKeyContent); err != nil {
		t.Fatal(err)
	}
	if publicKey, err = jwt.ParseRSAPublicKeyFromPEM(publicKeyContent); err != nil {
		t.Fatal(err)
	}
	token := jwt.New(jwt.GetSigningMethod("RS256"))

	token.Claims = jwt.MapClaims{
		"PERMISSION":       "admin@tiedot",
		JWT_ENDPOINTS_ATTR: []interface{}{},
		"exp":              time.Now().Add(time.Hour * 72).Unix(),
	}
	ts, err := token.SignedString(privateKey)

	req := httptest.NewRequest("GET", fmt.Sprintf(jwtUrlWithToken, ts), nil)
	w := httptest.NewRecorder()
	jwtWrap(func(w http.ResponseWriter, r *http.Request) {

	})(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Error("Expected status 401")
	}
}
func TestSliceContainsStr(t *testing.T) {
	if !sliceContainsStr([]string{"test"}, "test") {
		t.Error("Expected true from function `sliceContainsStr`")
	}

	if sliceContainsStr("test", "test") {
		t.Error("Expected false from function `sliceContainsStr`")
	}
}
