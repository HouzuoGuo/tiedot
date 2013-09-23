/* Bootstrap V3 server. */
package v3

import (
	"fmt"
	"log"
	"loveoneanother.at/tiedot/db"
	"net/http"
	"sync"
	"time"
)

var V3DB *db.DB
var V3Sync = new(sync.RWMutex)

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

	/* Certain handlers are synchronized via mutex to guarantee safety, such as scrubbing a collection or dropping a collection.
	Other handlers are asynchronized for maximum concurrency. */

	// collection management (synchronized)
	http.HandleFunc("/create", Create)
	http.HandleFunc("/rename", Rename)
	http.HandleFunc("/drop", Drop)
	http.HandleFunc("/all", All)
	http.HandleFunc("/scrub", Scrub)
	// query (asynchronized)
	http.HandleFunc("/query", Query)
	http.HandleFunc("/queryID", QueryID)
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
	// misc (asynchronized)
	http.HandleFunc("/version", Version)
	http.HandleFunc("/memstats", MemStats)
	// flush all buffers every minute
	go func() {
		ticker := time.Tick(time.Minute)
		for _ = range ticker {
			V3DB.Flush()
			log.Printf("Buffers flushed at %s", time.Now())
		}
	}()
	log.Printf("Listening on all interfaces, port %d", port)
	http.ListenAndServe(fmt.Sprintf(":%d", port), nil)
}
