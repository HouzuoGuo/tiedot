/* Miscellaneous function handlers. */
package v1

import (
	"net/http"
	"os"
)

// Flush and close all data files and shutdown entire program.
func Shutdown(w http.ResponseWriter, r *http.Request) {
	V1Sync.Lock()
	defer V1Sync.Unlock()
	w.Header().Set("Cache-Control", "must-revalidate")
	w.Header().Set("Content-Type", "application/json")
	V1DB.Close()
	os.Exit(0)
}

// Return server protocol version number.
func Version(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "must-revalidate")
	w.Header().Set("Content-Type", "text/plain")
	w.Write([]byte("1"))
}
