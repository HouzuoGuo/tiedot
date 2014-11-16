/* HTTP service API handler registration. */
package httpapi

import (
	"fmt"
	"github.com/HouzuoGuo/tiedot/db"
	"github.com/HouzuoGuo/tiedot/tdlog"
	"github.com/HouzuoGuo/tiedot/webcp"
	"net/http"
)

var (
	HttpDB  *db.DB
	jwtFlag bool
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

func Start(db *db.DB, port int, jwtFlag bool) {
	HttpDB = db

	// collection management (stop-the-world)
	http.HandleFunc("/create", wrap(Create, jwtFlag))
	http.HandleFunc("/rename", wrap(Rename, jwtFlag))
	http.HandleFunc("/drop", wrap(Drop, jwtFlag))
	http.HandleFunc("/all", wrap(All, jwtFlag))
	http.HandleFunc("/scrub", wrap(Scrub, jwtFlag))
	http.HandleFunc("/sync", wrap(Sync, jwtFlag))
	// query
	http.HandleFunc("/query", wrap(Query, jwtFlag))
	http.HandleFunc("/count", wrap(Count, jwtFlag))
	// document management
	http.HandleFunc("/insert", wrap(Insert, jwtFlag))
	http.HandleFunc("/get", wrap(Get, jwtFlag))
	http.HandleFunc("/getpage", wrap(GetPage, jwtFlag))
	http.HandleFunc("/update", wrap(Update, jwtFlag))
	http.HandleFunc("/delete", wrap(Delete, jwtFlag))
	http.HandleFunc("/approxdoccount", wrap(ApproxDocCount, jwtFlag))
	// index management (stop-the-world)
	http.HandleFunc("/index", wrap(Index, jwtFlag))
	http.HandleFunc("/indexes", wrap(Indexes, jwtFlag))
	http.HandleFunc("/unindex", wrap(Unindex, jwtFlag))
	// misc (stop-the-world)
	http.HandleFunc("/shutdown", wrap(Shutdown, jwtFlag))
	http.HandleFunc("/dump", wrap(Dump, jwtFlag))
	// misc
	http.HandleFunc("/version", wrap(Version, jwtFlag))
	http.HandleFunc("/memstats", wrap(MemStats, jwtFlag))
	// web control panel
	webcp.RegisterWebCp()

	tdlog.Noticef("Will listen on all interfaces, port %d", port)
	if jwtFlag == false {
		http.ListenAndServe(fmt.Sprintf(":%d", port), nil)
	} else {
		//openssl req -new -key rsa -out rsa.crt -x509 -days 3650 -subj "/C=/ST=/L=Earth/O=Tiedot/OU=IT/CN=localhost/emailAddress=admin@tiedot"
		http.HandleFunc("/getJwt", getJwt)
	    http.HandleFunc("/checkJwt", checkJwt)
		if e := http.ListenAndServeTLS(fmt.Sprintf(":%d", port), "rsa.crt", "rsa", nil); e != nil {
			tdlog.Noticef("%s", e)
		}
	}
}
