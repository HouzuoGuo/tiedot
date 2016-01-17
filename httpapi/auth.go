/*
This package enables the use of a token-based Authorization header.

In order for requests to be valid, clients must set the following request header:
    "Authorization: token TOKEN"
where TOKEN is the value set via the -authtoken flag in the command line.

Server usage example:
    tiedot -mode=httpd -dir=tmp/db -authtoken=abc123

Client request example:
    curl -I -H "Authorization: token abc123" http://127.0.0.1:8080/all
*/

package httpapi

import (
	"github.com/HouzuoGuo/tiedot/tdlog"
	"net/http"
)

var authToken string

// Enable Authorization header check on the HTTP handler function.
func authWrap(originalHandler http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if "token "+authToken != r.Header.Get("Authorization") {
			http.Error(w, "", http.StatusUnauthorized)
			return
		}
		originalHandler(w, r)
	}
}

func ServeAuthTokenEndpoints(token string) {
	authToken = token
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
	// misc
	http.HandleFunc("/shutdown", authWrap(Shutdown))
	http.HandleFunc("/dump", authWrap(Dump))

	tdlog.Noticef("API endpoints now require the 'Authorization: token' header.")
}
