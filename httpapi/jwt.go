/*
JWT stands for JSON Web Token.
The package creates, serves, and verifies JWT used by HTTP clients.

JWT authentication identities are stored in documents of collection "jwt", each document record should look like:
{
	"id": "the_login_identity",
	"password": "plain_text_password",
	"endpoints": [
		"create",
		"drop",
		"insert",
		"query",
		"update",
		"other_api_endpoint_names..."
	],
	"collections: [
		"collection_name_A",
		"collection_name_B",
		"other_collection_names..."
	]
}

A JWT document record allows a user identity identified by "id" and "password" to call the specified API endpoints
on the specified collections.

The JWT identity collection "jwt", along with a special user identity "admin", are created upon startup.
If they are missing, they will be re-created automatically upon startup.

The special user identity "admin" allows access to all features and collection data. Its default password is empty string.
*/

package httpapi

import (
	//"crypto/sha1"
	//"encoding/base64"
	"fmt"
	"github.com/HouzuoGuo/tiedot/db"
	"github.com/HouzuoGuo/tiedot/tdlog"
	jwt "github.com/dgrijalva/jwt-go"
	"io/ioutil"
	"net/http"
	"reflect"
	"strings"
	"time"
)

var (
	privateKey []byte //openssl genrsa -out rsa 1024
	publicKey  []byte //openssl rsa -in rsa -pubout > rsa.pub
)

const (
	// JWT Record and claim
	JWT_COL_NAME         = "jwt"
	JWT_ID_ATTR          = "id"
	JWT_PASS_ATTR        = "password"
	JWT_ENDPOINTS_ATTR   = "endpoints"
	JWT_COLLECTIONS_ATTR = "collections"
	JWT_ADMIN_ID         = "admin"
	// JWT claim
	JWT_EXPIRY = "exp"
)

// If necessary, create the JWT identity collection, indexes, and the default/special user identity "admin".
func jwtInitSetup() {
	// Create collection
	if HttpDB.Use(JWT_COL_NAME) == nil {
		if err := HttpDB.Create(JWT_COL_NAME); err != nil {
			tdlog.Panicf("JWT: failed to create JWT identity collection - %v", err)
		}
	}
	jwtCol := HttpDB.Use(JWT_COL_NAME)
	// Create indexes on ID attribute
	indexPaths := make(map[string]struct{})
	for _, oneIndex := range jwtCol.AllIndexes() {
		indexPaths[strings.Join(oneIndex, db.INDEX_PATH_SEP)] = struct{}{}
	}
	if _, exists := indexPaths[JWT_ID_ATTR]; !exists {
		if err := jwtCol.Index([]string{JWT_ID_ATTR}); err != nil {
			tdlog.Panicf("JWT: failed to create collection index - %v", err)
		}
	}
	// Create default user "admin"
	adminQuery := map[string]interface{}{
		"eq": JWT_ADMIN_ID,
		"in": []interface{}{JWT_ID_ATTR}}
	adminQueryResult := make(map[int]struct{})
	if err := db.EvalQuery(adminQuery, jwtCol, &adminQueryResult); err != nil {
		tdlog.Panicf("JWT: failed to query admin user ID - %v", err)
	}
	if len(adminQueryResult) == 0 {
		if _, err := jwtCol.Insert(map[string]interface{}{
			JWT_ID_ATTR:   JWT_ADMIN_ID,
			JWT_PASS_ATTR: "z4PhNX7vuL3xVChQ1m2AB9Yg5AULVxXcg/SpIdNs6c5H0NE8XYXysP+DGNKHfuwvY7kxvUdBeoGlODJ6+SfaPg=="}); err != nil { // Pass is empty string
			tdlog.Panicf("JWT: failed to create default admin user - %v", err)
		}
	}
}

// Verify user identity and hand out a JWT.
func getJwt(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "must-revalidate")
	w.Header().Set("Content-Type", "application/json")
	// Verify identity
	id := r.FormValue(JWT_ID_ATTR)
	if id == "" {
		http.Error(w, "Please pass JWT 'id' parameter", 400)
		return
	}
	jwtCol := HttpDB.Use(JWT_COL_NAME)
	if jwtCol == nil {
		http.Error(w, "Server is missing JWT identity collection, please restart the server.", 500)
		return
	}
	idQuery := map[string]interface{}{
		"eq": id,
		"in": []interface{}{JWT_ID_ATTR}}
	idQueryResult := make(map[int]struct{})
	if err := db.EvalQuery(idQuery, jwtCol, &idQueryResult); err != nil {
		tdlog.CritNoRepeat("JWT identity collection query failed: %v", err)
		http.Error(w, "JWT identity collection query failed", 500)
		return
	}
	// Verify password
	// sha := sha1.Sum([]byte(r.FormValue("password")))
	// pass := base64.URLEncoding.EncodeToString(sha[:20])
	pass := r.FormValue("password")
	for recID, _ := range idQueryResult {
		rec, err := jwtCol.Read(recID)
		if err != nil {
			break
		}
		if rec[JWT_PASS_ATTR] != pass {
			tdlog.CritNoRepeat("JWT: identitify verification failed from request sent by %s", r.RemoteAddr)
			break
		}
		// Successful password match
		token := jwt.New(jwt.GetSigningMethod("RS256"))
		token.Claims[JWT_ID_ATTR] = rec[JWT_ID_ATTR]
		token.Claims[JWT_COLLECTIONS_ATTR] = rec[JWT_COLLECTIONS_ATTR]
		token.Claims[JWT_ENDPOINTS_ATTR] = rec[JWT_ENDPOINTS_ATTR]
		token.Claims[JWT_EXPIRY] = time.Now().Add(time.Hour * 72).Unix()
		var tokenString string
		var e error
		if tokenString, e = token.SignedString(privateKey); e != nil {
			panic(e)
		}
		w.Header().Set("Authorization", "Bearer "+tokenString)
		w.WriteHeader(http.StatusOK)
		return
	}
	// ... password mismatch
	http.Error(w, "Invalid password", 401)
}

func checkJwt(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	token, err := jwt.ParseFromRequest(r, func(token *jwt.Token) (interface{}, error) {
		return publicKey, nil
	})
	if token.Valid {
		//log.Print(token)
		//fmt.Fprintf(w, "{\"object\": %v}", token)
	} else {
		tdlog.Notice(err)
		fmt.Fprintf(w, "{\"error\": \"%s %s\"}", "JWT not valid,", err)
	}
}

func wrap(fn http.HandlerFunc, jwtFlag bool) http.HandlerFunc {
	if jwtFlag == false {
		return fn
	}
	var e error
	if privateKey, e = ioutil.ReadFile("rsa"); e != nil {
		tdlog.Panicf("%s", e)
	}
	if publicKey, e = ioutil.ReadFile("rsa.pub"); e != nil {
		tdlog.Panicf("%s", e)
	}
	return func(w http.ResponseWriter, r *http.Request) {
		if origin := r.Header.Get("Origin"); origin != "" {
			w.Header().Set("Access-Control-Allow-Origin", origin)
		}
		w.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS, PUT, DELETE")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Content-Length, Accept-Encoding, X-CSRF-Token, Authorization")
		//w.Header().Set("Access-Control-Allow-Credentials", "true")
		//w.Header().Set("Content-Type", "application/json")
		t, _ := jwt.ParseFromRequest(r, func(token *jwt.Token) (interface{}, error) {
			return publicKey, nil
		})
		if t == nil {
			return
		}
		if !t.Valid {
			return
		}
		if t.Claims[JWT_ADMIN_ID] == JWT_ADMIN_ID {
			fn(w, r)
			return
		}
		var url = strings.TrimPrefix(r.URL.Path, "/")
		if !test(t.Claims[JWT_ENDPOINTS_ATTR], url) {
			return
		}
		var col = r.FormValue("col")
		if col != "" && !test(t.Claims[JWT_COLLECTIONS_ATTR], col) {
			return
		}
		//tdlog.Notice(t)
		//tdlog.Notice(url, " ", col)
		fn(w, r)
	}
}

func test(t interface{}, v string) bool {
	switch reflect.TypeOf(t).Kind() {
	case reflect.Slice:
		s := reflect.ValueOf(t)
		for i := 0; i < s.Len(); i++ {
			if s.Index(i).Interface() == v {
				return true
			}
		}
	}
	tdlog.Noticef("Test fails for %s.", v)
	return false
}
