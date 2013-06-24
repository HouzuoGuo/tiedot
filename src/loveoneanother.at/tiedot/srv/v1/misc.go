/* Miscellaneous function handlers. */
package v1

import (
	"net/http"
)

func Shutdown(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "must-revalidate")
	V1DB.Close()
}
