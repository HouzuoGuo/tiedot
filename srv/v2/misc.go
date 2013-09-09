/* Miscellaneous function handlers. */
package v2

import (
	"encoding/json"
	"net/http"
	"os"
	"runtime"
)

// Flush and close all data files and shutdown entire program.
func Shutdown(w http.ResponseWriter, r *http.Request) {
	V2Sync.Lock()
	defer V2Sync.Unlock()
	w.Header().Set("Cache-Control", "must-revalidate")
	w.Header().Set("Content-Type", "application/json")
	V2DB.Close()
	os.Exit(0)
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
	w.Write([]byte("2"))
}
