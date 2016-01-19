// Miscellaneous function handlers.

package httpapi

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"runtime"
)

// Flush and close all data files and shutdown the entire program.
func Shutdown(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "must-revalidate")
	w.Header().Set("Content-Type", "text/plain")
	HttpDB.Close()
	os.Exit(0)
}

// Copy this database into destination directory.
func Dump(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "must-revalidate")
	w.Header().Set("Content-Type", "text/plain")
	var dest string
	if !Require(w, r, "dest", &dest) {
		return
	}
	if err := HttpDB.Dump(dest); err != nil {
		http.Error(w, fmt.Sprint(err), 500)
		return
	}
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
	w.Write([]byte("6"))
}
