/* HTTP service API handler registration. */
package httpapi

import (
	"fmt"
	"github.com/HouzuoGuo/tiedot/db"
	"github.com/HouzuoGuo/tiedot/tdlog"
	"github.com/HouzuoGuo/tiedot/webcp"
	"github.com/HouzuoGuo/tiedot/webjwt"
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
	http.HandleFunc("/create", webjwt.Wrap(Create, jwtFlag))
	http.HandleFunc("/rename", webjwt.Wrap(Rename, jwtFlag))
	http.HandleFunc("/drop", webjwt.Wrap(Drop, jwtFlag))
	http.HandleFunc("/all", webjwt.Wrap(All, jwtFlag))
	http.HandleFunc("/scrub", webjwt.Wrap(Scrub, jwtFlag))
	http.HandleFunc("/sync", webjwt.Wrap(Sync, jwtFlag))

	// query
	http.HandleFunc("/query", webjwt.Wrap(Query, jwtFlag))
	http.HandleFunc("/count", webjwt.Wrap(Count, jwtFlag))

	// document management
	http.HandleFunc("/insert", webjwt.Wrap(Insert, jwtFlag))
	http.HandleFunc("/get", webjwt.Wrap(Get, jwtFlag))
	http.HandleFunc("/getpage", webjwt.Wrap(GetPage, jwtFlag))
	http.HandleFunc("/update", webjwt.Wrap(Update, jwtFlag))
	http.HandleFunc("/delete", webjwt.Wrap(Delete, jwtFlag))
	http.HandleFunc("/approxdoccount", webjwt.Wrap(ApproxDocCount, jwtFlag))

	// index management (stop-the-world)
	http.HandleFunc("/index", webjwt.Wrap(Index, jwtFlag))
	http.HandleFunc("/indexes", webjwt.Wrap(Indexes, jwtFlag))
	http.HandleFunc("/unindex", webjwt.Wrap(Unindex, jwtFlag))

	// misc (stop-the-world)
	http.HandleFunc("/shutdown", webjwt.Wrap(Shutdown, jwtFlag))
	http.HandleFunc("/dump", webjwt.Wrap(Dump, jwtFlag))

	// misc
	http.HandleFunc("/version", webjwt.Wrap(Version, jwtFlag))
	http.HandleFunc("/memstats", webjwt.Wrap(MemStats, jwtFlag))

	// web control panel
	tdlog.Noticef("Will listen on all interfaces, port %d", port)
	if jwtFlag == false {
		webcp.RegisterWebCp()
		http.ListenAndServe(fmt.Sprintf(":%d", port), nil)
	} else {
		webjwt.RegisterWebJwt(HttpDB)
		//openssl req -new -key rsa -out rsa.crt -x509 -days 3650 -subj "/C=/ST=/L=Earth/O=Tiedot/OU=IT/CN=localhost/emailAddress=admin@tiedot"
		if e := http.ListenAndServeTLS(fmt.Sprintf(":%d", port), "rsa.crt", "rsa", nil); e != nil {
			tdlog.Noticef("%s", e)
		}
	}
}
