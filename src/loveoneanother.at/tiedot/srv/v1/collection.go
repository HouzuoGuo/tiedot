/* Collection management handlers. */
package v1

import (
	"net/http"
)

func Create(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte(r.FormValue("name")))
}

func Rename(w http.ResponseWriter, r *http.Request) {
}

func Drop(w http.ResponseWriter, r *http.Request) {
}
