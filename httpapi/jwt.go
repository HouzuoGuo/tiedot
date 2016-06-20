/*
JWT stands for JSON Web Token.
The package creates, serves, and verifies JWT used by HTTP clients.

JWT user ID and access rights are stored in documents of collection "jwt", each document record should look like:
{
    "user": "user_name",
    "pass": "password_plain_text",
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

A JWT document record allows a user identity identified by "user" and "pass" to call the specified API endpoints
on the specified collections.

The JWT identity collection "jwt", along with a special user identity "admin", are created upon startup.
If they are missing, they will be re-created automatically upon startup.

The special user identity "admin" allows access to all features and collection data. Its default password is empty string.
*/

package httpapi

import (
	"fmt"
	"net/http"
	"strings"
	"time"

	"crypto/rsa"
	"github.com/HouzuoGuo/tiedot/db"
	"github.com/HouzuoGuo/tiedot/tdlog"
	jwt "github.com/dgrijalva/jwt-go"
	"github.com/dgrijalva/jwt-go/request"
)

var (
	privateKey *rsa.PrivateKey //openssl genrsa -out rsa 1024
	publicKey  *rsa.PublicKey  //openssl rsa -in rsa -pubout > rsa.pub
)

const (
	// JWT Record and claim
	JWT_COL_NAME         = "jwt"
	JWT_USER_ATTR        = "user"
	JWT_PASS_ATTR        = "pass"
	JWT_ENDPOINTS_ATTR   = "endpoints"
	JWT_COLLECTIONS_ATTR = "collections"
	JWT_USER_ADMIN       = "admin"
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
	if _, exists := indexPaths[JWT_USER_ATTR]; !exists {
		if err := jwtCol.Index([]string{JWT_USER_ATTR}); err != nil {
			tdlog.Panicf("JWT: failed to create collection index - %v", err)
		}
	}
	// Create default user "admin"
	adminQuery := map[string]interface{}{
		"eq": JWT_USER_ADMIN,
		"in": []interface{}{JWT_USER_ATTR}}
	adminQueryResult := make(map[int]struct{})
	if err := db.EvalQuery(adminQuery, jwtCol, &adminQueryResult); err != nil {
		tdlog.Panicf("JWT: failed to query admin user ID - %v", err)
	}
	if len(adminQueryResult) == 0 {
		if _, err := jwtCol.Insert(map[string]interface{}{
			JWT_USER_ATTR:        JWT_USER_ADMIN,
			JWT_PASS_ATTR:        "",
			JWT_COLLECTIONS_ATTR: []interface{}{},
			JWT_ENDPOINTS_ATTR:   []interface{}{}}); err != nil {
			tdlog.Panicf("JWT: failed to create default admin user - %v", err)
		}
		tdlog.Notice("JWT: successfully initialized DB for JWT features. The default user 'admin' has been created.")
	}
}

// Enforce must-revalidate cache control, and configure response headers for CORS operation.
func addCommonJwtRespHeaders(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "must-revalidate")
	if origin := r.Header.Get("Origin"); origin != "" {
		w.Header().Set("Access-Control-Allow-Origin", origin)
	}
	w.Header().Set("Access-Control-Expose-Headers", "Authorization")
	w.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS, PUT, DELETE")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Content-Length, Accept-Encoding, X-CSRF-Token, Authorization")
}

// Verify user identity and hand out a JWT.
func getJWT(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	addCommonJwtRespHeaders(w, r)
	// Verify identity
	user := r.FormValue(JWT_USER_ATTR)
	if user == "" {
		http.Error(w, "Please pass JWT 'user' parameter", http.StatusBadRequest)
		return
	}
	jwtCol := HttpDB.Use(JWT_COL_NAME)
	if jwtCol == nil {
		http.Error(w, "Server is missing JWT identity collection, please restart the server.", http.StatusInternalServerError)
		return
	}
	userQuery := map[string]interface{}{
		"eq": user,
		"in": []interface{}{JWT_USER_ATTR}}
	userQueryResult := make(map[int]struct{})
	if err := db.EvalQuery(userQuery, jwtCol, &userQueryResult); err != nil {
		tdlog.CritNoRepeat("Query failed in JWT identity collection : %v", err)
		http.Error(w, "Query failed in JWT identity collection", http.StatusInternalServerError)
		return
	}
	// Verify password
	pass := r.FormValue(JWT_PASS_ATTR)
	for recID := range userQueryResult {
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
		token.Claims = jwt.MapClaims{
			JWT_USER_ATTR:        rec[JWT_USER_ATTR],
			JWT_COLLECTIONS_ATTR: rec[JWT_COLLECTIONS_ATTR],
			JWT_ENDPOINTS_ATTR:   rec[JWT_ENDPOINTS_ATTR],
			JWT_EXPIRY:           time.Now().Add(time.Hour * 72).Unix(),
		}
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

// Extract JWT from Authorization header or "access_token" attribute.
type TokenExtractor struct {
}

func (t TokenExtractor) ExtractToken(req *http.Request) (string, error) {
	token := req.Header.Get("Authorization")
	if token == "" {
		token = req.FormValue("access_token")
	}
	if token == "" {
		return "", request.ErrNoTokenInRequest
	}
	// For the sake of simplicity, extra spaces and type name Bearer are stripped.
	return strings.TrimSpace(strings.TrimPrefix(token, "Bearer")), nil
}

// Verify user's JWT.
func checkJWT(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	addCommonJwtRespHeaders(w, r)
	// Look for JWT in both headers and request value "access_token".
	token, err := request.ParseFromRequest(r, TokenExtractor{}, func(token *jwt.Token) (interface{}, error) {
		// Token was signed with RSA method when it was initially granted
		if _, ok := token.Method.(*jwt.SigningMethodRSA); !ok {
			return nil, fmt.Errorf("Unexpected signing method: %v", token.Header["alg"])
		}
		return publicKey, nil
	})
	if err != nil || !token.Valid {
		http.Error(w, fmt.Sprintf("{\"error\": \"%s %s\"}", "JWT not valid,", err), http.StatusUnauthorized)
	} else {
		w.WriteHeader(http.StatusOK)
	}
}

// Enable JWT authorization check on the HTTP handler function.
func jwtWrap(originalHandler http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		addCommonJwtRespHeaders(w, r)
		// Look for JWT in both headers and request value "access_token".
		token, err := request.ParseFromRequest(r, TokenExtractor{}, func(token *jwt.Token) (interface{}, error) {
			// Token was signed with RSA method when it was initially granted
			if _, ok := token.Method.(*jwt.SigningMethodRSA); !ok {
				return nil, fmt.Errorf("Unexpected signing method: %v", token.Header["alg"])
			}
			return publicKey, nil
		})
		if err != nil || !token.Valid {
			http.Error(w, "", http.StatusUnauthorized)
			return
		}
		tokenClaims := token.Claims.(jwt.MapClaims)
		var url = strings.TrimPrefix(r.URL.Path, "/")
		var col = r.FormValue("col")
		// Call the API endpoint handler if authorization allows
		if tokenClaims[JWT_USER_ATTR] == JWT_USER_ADMIN {
			originalHandler(w, r)
			return
		}
		if !sliceContainsStr(tokenClaims[JWT_ENDPOINTS_ATTR], url) {
			http.Error(w, "", http.StatusUnauthorized)
			return
		} else if col != "" && !sliceContainsStr(tokenClaims[JWT_COLLECTIONS_ATTR], col) {
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
