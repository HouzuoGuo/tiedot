/* Miscellaneous function handlers. */
package httpapi

import (
	"encoding/json"
	"net/http"
	"os"
	"runtime"
)

// Flush and close all data files and shutdown entire program.
func Shutdown(w http.ResponseWriter, r *http.Request) {
	HttpDBSync.Lock()
	defer HttpDBSync.Unlock()
	w.Header().Set("Cache-Control", "must-revalidate")
	w.Header().Set("Content-Type", "application/json")
	HttpDB.Close()
	os.Exit(0)
}

// Pause all activities and make a dump of entire database to another file system location.
func Dump(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "must-revalidate")
	w.Header().Set("Content-Type", "application/json")
	var dest string
	if !Require(w, r, "dest", &dest) {
		return
	}
	HttpDBSync.Lock()
	defer HttpDBSync.Unlock()
	// TODO: implement me
}

// Return server memory statistics.
func MemStats(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "must-revalidate")
	w.Header().Set("Content-Type", "application/json")
	stats := new(runtime.MemStats)
	runtime.ReadMemStats(stats)
	resp, err := json.Marshal(stats)
	if err != nil {
		http.Error(w, "Cannot serialize MemStats to JSON.", 500)
		return
	}
	w.Write(resp)
}

// Return server protocol version number.
func Version(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "must-revalidate")
	w.Header().Set("Content-Type", "text/plain")
	w.Write([]byte("3"))
}
