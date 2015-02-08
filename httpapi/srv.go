/* HTTP service API handler registration. */
package httpapi

import (
	"fmt"
	"github.com/HouzuoGuo/tiedot/db"
	"github.com/HouzuoGuo/tiedot/tdlog"
	"net/http"
)

var (
	HttpDB *db.DB
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

func Start(dir string, port int, tlsCrt, tlsKey, jwtPubKey, jwtPrivateKey string) {
	var err error
	HttpDB, err = db.OpenDB(dir)
	if err != nil {
		panic(err)
	}

	// These endpoints are always available and do not require JWT auth
	http.HandleFunc("/", Welcome)
	http.HandleFunc("/version", Version)
	http.HandleFunc("/memstats", MemStats)

	if jwtPrivateKey != "" {
		// JWT support
		ServeJWTEnabledEndpoints(jwtPubKey, jwtPrivateKey)
	} else {
		// No JWT
		ServeEndpoints()
	}

	if tlsCrt != "" {
		tdlog.Noticef("Will listen on all interfaces (HTTPS), port %d.", port)
		if err := http.ListenAndServeTLS(fmt.Sprintf(":%d", port), tlsCrt, tlsKey, nil); err != nil {
			tdlog.Panicf("Failed to start HTTPS service - %s", err)
		}
	} else {
		tdlog.Noticef("Will listen on all interfaces (HTTP), port %d.", port)
		http.ListenAndServe(fmt.Sprintf(":%d", port), nil)
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

func ServeEndpoints() {
	// collection management (stop-the-world)
	http.HandleFunc("/create", Create)
	http.HandleFunc("/rename", Rename)
	http.HandleFunc("/drop", Drop)
	http.HandleFunc("/all", All)
	http.HandleFunc("/scrub", Scrub)
	http.HandleFunc("/sync", Sync)
	// query
	http.HandleFunc("/query", Query)
	http.HandleFunc("/count", Count)
	// document management
	http.HandleFunc("/insert", Insert)
	http.HandleFunc("/get", Get)
	http.HandleFunc("/getpage", GetPage)
	http.HandleFunc("/getall", GetAll)
	http.HandleFunc("/update", Update)
	http.HandleFunc("/delete", Delete)
	http.HandleFunc("/approxdoccount", ApproxDocCount)
	// index management (stop-the-world)
	http.HandleFunc("/index", Index)
	http.HandleFunc("/indexes", Indexes)
	http.HandleFunc("/unindex", Unindex)
	// misc (stop-the-world)
	http.HandleFunc("/shutdown", Shutdown)
	http.HandleFunc("/dump", Dump)
}
