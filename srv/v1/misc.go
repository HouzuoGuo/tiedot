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
	V1DB.Close()
	os.Exit(0)
}
