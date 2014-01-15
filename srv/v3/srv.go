/* Bootstrap V3 server. */
package v3

import (
	"fmt"
	"github.com/HouzuoGuo/tiedot/db"
	"github.com/HouzuoGuo/tiedot/tdlog"
	"net/http"
	"sync"
	"time"
)

var V3DB *db.DB
var V3Sync = new(sync.RWMutex) // To synchronize "stop-the-world" operations

// Store form parameter value of specified key to *val and return true; if key does not exist, set HTTP status 400 and return false.
func Require(w http.ResponseWriter, r *http.Request, key string, val *string) bool {
	*val = r.FormValue(key)
	if *val == "" {
		http.Error(w, fmt.Sprintf("Please pass POST/PUT/GET parameter value of '%s'.", key), 400)
		return false
	}
	return true
}

func Start(db *db.DB, port int) {
	V3DB = db

	// collection management (synchronized)
	http.HandleFunc("/create", Create)
	http.HandleFunc("/rename", Rename)
	http.HandleFunc("/drop", Drop)
	http.HandleFunc("/all", All)
	http.HandleFunc("/scrub", Scrub)
	http.HandleFunc("/repartition", Repartition)
	http.HandleFunc("/flush", Flush)
	// query (asynchronized)
	http.HandleFunc("/query", Query)
	http.HandleFunc("/count", Count)
	// document management (asynchronized)
	http.HandleFunc("/insert", Insert)
	http.HandleFunc("/get", Get)
	http.HandleFunc("/update", Update)
	http.HandleFunc("/delete", Delete)
	// index management (synchronized)
	http.HandleFunc("/index", Index)
	http.HandleFunc("/indexes", Indexes)
	http.HandleFunc("/unindex", Unindex)
	// misc (synchronized)
	http.HandleFunc("/shutdown", Shutdown)
	http.HandleFunc("/dump", Dump)
	// misc (asynchronized)
	http.HandleFunc("/version", Version)
	http.HandleFunc("/memstats", MemStats)
	// flush all buffers every 10 seconds
	go func() {
		ticker := time.Tick(time.Second * 10)
		for _ = range ticker {
			V3DB.Flush()
		}
	}()
	tdlog.Printf("Listening on all interfaces, port %d", port)
	http.ListenAndServe(fmt.Sprintf(":%d", port), nil)
}
