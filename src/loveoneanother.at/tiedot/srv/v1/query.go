/* Query handlers. */
package v1

import (
	"net/http"
)

func Select(w http.ResponseWriter, r *http.Request) {
	V1Sync.RLock()
	defer V1Sync.RUnlock()
}

func Find(w http.ResponseWriter, r *http.Request) {
	V1Sync.RLock()
	defer V1Sync.RUnlock()
}

func Count(w http.ResponseWriter, r *http.Request) {
	V1Sync.RLock()
	defer V1Sync.RUnlock()
}
