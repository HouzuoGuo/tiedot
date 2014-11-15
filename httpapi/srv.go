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
	http.HandleFunc("/create", webcp.Wrap(Create, jwtFlag))
	http.HandleFunc("/rename", webcp.Wrap(Rename, jwtFlag))
	http.HandleFunc("/drop", webcp.Wrap(Drop, jwtFlag))
	http.HandleFunc("/all", webcp.Wrap(All, jwtFlag))
	http.HandleFunc("/scrub", webcp.Wrap(Scrub, jwtFlag))
	http.HandleFunc("/sync", webcp.Wrap(Sync, jwtFlag))
	// query
	http.HandleFunc("/query", webcp.Wrap(Query, jwtFlag))
	http.HandleFunc("/count", webcp.Wrap(Count, jwtFlag))
	// document management
	http.HandleFunc("/insert", webcp.Wrap(Insert, jwtFlag))
	http.HandleFunc("/get", webcp.Wrap(Get, jwtFlag))
	http.HandleFunc("/getpage", webcp.Wrap(GetPage, jwtFlag))
	http.HandleFunc("/update", webcp.Wrap(Update, jwtFlag))
	http.HandleFunc("/delete", webcp.Wrap(Delete, jwtFlag))
	http.HandleFunc("/approxdoccount", webcp.Wrap(ApproxDocCount, jwtFlag))
	// index management (stop-the-world)
	http.HandleFunc("/index", webcp.Wrap(Index, jwtFlag))
	http.HandleFunc("/indexes", webcp.Wrap(Indexes, jwtFlag))
	http.HandleFunc("/unindex", webcp.Wrap(Unindex, jwtFlag))
	// misc (stop-the-world)
	http.HandleFunc("/shutdown", webcp.Wrap(Shutdown, jwtFlag))
	http.HandleFunc("/dump", webcp.Wrap(Dump, jwtFlag))
	// misc
	http.HandleFunc("/version", webcp.Wrap(Version, jwtFlag))
	http.HandleFunc("/memstats", webcp.Wrap(MemStats, jwtFlag))
	// web control panel
	webcp.RegisterWebCp(HttpDB)
	
	tdlog.Noticef("Will listen on all interfaces, port %d", port)
	if jwtFlag == false {
		http.ListenAndServe(fmt.Sprintf(":%d", port), nil)
	} else {
		//openssl req -new -key rsa -out rsa.crt -x509 -days 3650 -subj "/C=/ST=/L=Earth/O=Tiedot/OU=IT/CN=localhost/emailAddress=admin@tiedot"
		if e := http.ListenAndServeTLS(fmt.Sprintf(":%d", port), "rsa.crt", "rsa", nil); e != nil {
			tdlog.Noticef("%s", e)
		}
	}
}
