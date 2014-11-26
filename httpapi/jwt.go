/*
JWT stands for JSON Web Token.
The package creates, serves, and verifies JWT used by HTTP clients.

JWT authentication identities are stored in documents of collection "jwt", each document record should look like:
{
	"id": "the_login_identity",
	"password": "hashed_password_sha512",
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
			JWT_ID_ATTR: JWT_ADMIN_ID,
			// Pass is SHA512 of empty string
			JWT_PASS_ATTR:        "z4PhNX7vuL3xVChQ1m2AB9Yg5AULVxXcg/SpIdNs6c5H0NE8XYXysP+DGNKHfuwvY7kxvUdBeoGlODJ6+SfaPg==",
			JWT_COLLECTIONS_ATTR: []interface{}{},
			JWT_ENDPOINTS_ATTR:   []interface{}{}}); err != nil {
			tdlog.Panicf("JWT: failed to create default admin user - %v", err)
		}
		tdlog.Notice("JWT: initialization ran successfully, the default user 'admin' has been created.")
	}
}

// Verify user identity and hand out a JWT.
func getJwt(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "must-revalidate")
	w.Header().Set("Content-Type", "application/json")
	// Verify identity
	id := r.FormValue(JWT_ID_ATTR)
	if id == "" {
		http.Error(w, "Please pass JWT 'id' parameter", http.StatusBadRequest)
		return
	}
	jwtCol := HttpDB.Use(JWT_COL_NAME)
	if jwtCol == nil {
		http.Error(w, "Server is missing JWT identity collection, please restart the server.", http.StatusInternalServerError)
		return
	}
	idQuery := map[string]interface{}{
		"eq": id,
		"in": []interface{}{JWT_ID_ATTR}}
	idQueryResult := make(map[int]struct{})
	if err := db.EvalQuery(idQuery, jwtCol, &idQueryResult); err != nil {
		tdlog.CritNoRepeat("JWT identity collection query failed: %v", err)
		http.Error(w, "JWT identity collection query failed", http.StatusInternalServerError)
		return
	}
	// Verify password
	// sha := sha1.Sum([]byte(r.FormValue("password")))
	// pass := base64.URLEncoding.EncodeToString(sha[:20])
	pass := r.FormValue(JWT_PASS_ATTR)
	// tdlog.Notice(pass)
	for recID, _ := range idQueryResult {
		rec, err := jwtCol.Read(recID)
		if err != nil {
			//tdlog.Notice(rec)
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
	http.Error(w, "Invalid password", http.StatusUnauthorized)
}

// Verify user's JWT.
func checkJwt(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	token, err := jwt.ParseFromRequest(r, func(token *jwt.Token) (interface{}, error) {
		return publicKey, nil
	})
	if token.Valid {
		w.WriteHeader(http.StatusOK)
	} else {
		http.Error(w, fmt.Sprintf("{\"error\": \"%s %s\"}", "JWT not valid,", err), http.StatusUnauthorized)
	}
}

// Enable JWT authorization check on the handler function.
func jwtWrap(originalHandler http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if origin := r.Header.Get("Origin"); origin != "" {
			w.Header().Set("Access-Control-Allow-Origin", origin)
		}
		w.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS, PUT, DELETE")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Content-Length, Accept-Encoding, X-CSRF-Token, Authorization")
		t, _ := jwt.ParseFromRequest(r, func(token *jwt.Token) (interface{}, error) {
			return publicKey, nil
		})
		if t == nil {
			http.Error(w, "", http.StatusUnauthorized)
			return
		} else if !t.Valid {
			http.Error(w, "", http.StatusUnauthorized)
			return
		}
		if t.Claims[JWT_ID_ATTR] == JWT_ADMIN_ID {
			originalHandler(w, r)
			return
		}
		var url = strings.TrimPrefix(r.URL.Path, "/")
		if !sliceContainsStr(t.Claims[JWT_ENDPOINTS_ATTR], url) {
			http.Error(w, "", http.StatusUnauthorized)
			return
		}
		var col = r.FormValue("col")
		if col != "" && !sliceContainsStr(t.Claims[JWT_COLLECTIONS_ATTR], col) {
			http.Error(w, "", http.StatusUnauthorized)
			return
		}
		originalHandler(w, r)
	}
}

// Return true if the string appears in string slice.
func sliceContainsStr(possibleSlice interface{}, str string) bool {
	switch possibleSlice.(type) {
	case []string:
		for _, elem := range possibleSlice.([]string) {
			if elem == str {
				return true
			}
		}
	}
	return false
}

func ServeJWTEnabledEndpoints(jwtPubKey, jwtPrivateKey string) {
	var e error
	if publicKey, e = ioutil.ReadFile(jwtPubKey); e != nil {
		tdlog.Panicf("JWT: Failed to read public key file - %s", e)
	}
	if privateKey, e = ioutil.ReadFile(jwtPrivateKey); e != nil {
		tdlog.Panicf("JWT: Failed to read private key file - %s", e)
	}

	jwtInitSetup()

	// collection management (stop-the-world)
	http.HandleFunc("/create", jwtWrap(Create))
	http.HandleFunc("/rename", jwtWrap(Rename))
	http.HandleFunc("/drop", jwtWrap(Drop))
	http.HandleFunc("/all", jwtWrap(All))
	http.HandleFunc("/scrub", jwtWrap(Scrub))
	http.HandleFunc("/sync", jwtWrap(Sync))
	// query
	http.HandleFunc("/query", jwtWrap(Query))
	http.HandleFunc("/count", jwtWrap(Count))
	// document management
	http.HandleFunc("/insert", jwtWrap(Insert))
	http.HandleFunc("/get", jwtWrap(Get))
	http.HandleFunc("/getpage", jwtWrap(GetPage))
	http.HandleFunc("/update", jwtWrap(Update))
	http.HandleFunc("/delete", jwtWrap(Delete))
	http.HandleFunc("/approxdoccount", jwtWrap(ApproxDocCount))
	// index management (stop-the-world)
	http.HandleFunc("/index", jwtWrap(Index))
	http.HandleFunc("/indexes", jwtWrap(Indexes))
	http.HandleFunc("/unindex", jwtWrap(Unindex))
	// misc
	http.HandleFunc("/shutdown", jwtWrap(Shutdown))
	http.HandleFunc("/dump", jwtWrap(Dump))
	http.HandleFunc("/getJwt", getJwt)
	http.HandleFunc("/checkJwt", checkJwt)

	tdlog.Noticef("JWT is enabled. API endpoints will require JWT authorization.")
}
