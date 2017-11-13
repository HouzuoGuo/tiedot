/*
Register HTTP API endpoints and handle authorization requirements.

Without specifying authorization parameters in the command line, tiedot server does not
require any authorization on any endpoint.

tiedot supports two authorization mechanisms:
- Pre-shared authorization token
The API endpoints will require 'Authorization: token PRE_SHARED_TOKEN' header. The pre-shared
token is specified in command line parameter "-authtoken".
Client request example: curl -I -H "Authorization: token PRE_SHARED_TOKEN" http://127.0.0.1:8080/all
- JWT (JSON Web Token)
The sophisticated mechanism offers finer-grained access control, separated by individual users.
Access to specific endpoints are granted explicitly to each user.

These API endpoints will never require authorization: / (root), /version, and /memstats
*/

package httpapi

import (
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/HouzuoGuo/tiedot/db"
	"github.com/HouzuoGuo/tiedot/tdlog"
	"github.com/dgrijalva/jwt-go"
)

var (
	HttpDB *db.DB // HTTP API endpoints operate on this database
)

// Store form parameter value of specified key to *val and return true; if key does not exist, set HTTP status 400 and return false.
func Require(w http.ResponseWriter, r *http.Request, key string, val *string) bool {
	*val = r.FormValue(key)
	if *val == "" {
		http.Error(w, fmt.Sprintf("Please pass POST/PUT/GET parameter value of '%s'.", key), 400)
		return false
	}
	return true
}

// Start HTTP server and block until the server shuts down. Panic on error.
func Start(dir string, port int, tlsCrt, tlsKey, jwtPubKey, jwtPrivateKey, bind, authToken string) {
	var err error
	HttpDB, err = db.OpenDB(dir)
	if err != nil {
		panic(err)
	}

	// These endpoints are always available and do not require authentication
	http.HandleFunc("/", Welcome)
	http.HandleFunc("/version", Version)
	http.HandleFunc("/memstats", MemStats)

	// Install API endpoint handlers that may require authorization
	var authWrap func(http.HandlerFunc) http.HandlerFunc
	if authToken != "" {
		tdlog.Noticef("API endpoints now require the pre-shared token in Authorization header.")
		authWrap = func(originalHandler http.HandlerFunc) http.HandlerFunc {
			return func(w http.ResponseWriter, r *http.Request) {
				if "token "+authToken != r.Header.Get("Authorization") {
					http.Error(w, "", http.StatusUnauthorized)
					return
				}
				originalHandler(w, r)
			}
		}
	} else if jwtPubKey != "" && jwtPrivateKey != "" {
		tdlog.Noticef("API endpoints now require JWT in Authorization header.")
		var publicKeyContent, privateKeyContent []byte
		if publicKeyContent, err = ioutil.ReadFile(jwtPubKey); err != nil {
			panic(err)
		} else if publicKey, err = jwt.ParseRSAPublicKeyFromPEM(publicKeyContent); err != nil {
			panic(err)
		} else if privateKeyContent, err = ioutil.ReadFile(jwtPrivateKey); err != nil {
			panic(err)
		} else if privateKey, err = jwt.ParseRSAPrivateKeyFromPEM(privateKeyContent); err != nil {
			panic(err)
		}
		jwtInitSetup()
		authWrap = jwtWrap
		// does not require JWT auth
		http.HandleFunc("/getjwt", getJWT)
		http.HandleFunc("/checkjwt", checkJWT)
	} else {
		tdlog.Noticef("API endpoints do not require Authorization header.")
		authWrap = func(originalHandler http.HandlerFunc) http.HandlerFunc {
			return originalHandler
		}
	}
	// collection management (stop-the-world)
	http.HandleFunc("/create", authWrap(Create))
	http.HandleFunc("/rename", authWrap(Rename))
	http.HandleFunc("/drop", authWrap(Drop))
	http.HandleFunc("/all", authWrap(All))
	http.HandleFunc("/scrub", authWrap(Scrub))
	http.HandleFunc("/sync", authWrap(Sync))
	// query
	http.HandleFunc("/query", authWrap(Query))
	http.HandleFunc("/count", authWrap(Count))
	// document management
	http.HandleFunc("/insert", authWrap(Insert))
	http.HandleFunc("/get", authWrap(Get))
	http.HandleFunc("/getpage", authWrap(GetPage))
	http.HandleFunc("/update", authWrap(Update))
	http.HandleFunc("/delete", authWrap(Delete))
	http.HandleFunc("/approxdoccount", authWrap(ApproxDocCount))
	// index management (stop-the-world)
	http.HandleFunc("/index", authWrap(Index))
	http.HandleFunc("/indexes", authWrap(Indexes))
	http.HandleFunc("/unindex", authWrap(Unindex))
	// misc (stop-the-world)
	http.HandleFunc("/shutdown", authWrap(Shutdown))
	http.HandleFunc("/dump", authWrap(Dump))

	iface := "all interfaces"
	if bind != "" {
		iface = bind
	}

	if tlsCrt != "" {
		tdlog.Noticef("Will listen on %s (HTTPS), port %d.", iface, port)
		if err := http.ListenAndServeTLS(fmt.Sprintf("%s:%d", bind, port), tlsCrt, tlsKey, nil); err != nil {
			tdlog.Panicf("Failed to start HTTPS service - %s", err)
		}
	} else {
		tdlog.Noticef("Will listen on %s (HTTP), port %d.", iface, port)
		http.ListenAndServe(fmt.Sprintf("%s:%d", bind, port), nil)
	}
}

// Greet user with a welcome message.
func Welcome(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.Error(w, "Invalid API endpoint", 404)
		return
	}
	w.Write([]byte("Welcome to tiedot"))
}
