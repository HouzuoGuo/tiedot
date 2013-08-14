/* Miscellaneous function handlers. */
package v2

import (
	"net/http"
	"os"
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

// Return server protocol version number.
func Version(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "must-revalidate")
	w.Header().Set("Content-Type", "text/plain")
	w.Write([]byte("1"))
}
