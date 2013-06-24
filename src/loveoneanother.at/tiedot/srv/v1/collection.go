/* Collection management handlers. */
package v1

import (
	"fmt"
	"net/http"
)

func Create(w http.ResponseWriter, r *http.Request) {
	var name string
	if !Require(w, r, "name", &name) {
		return
	}
	if err := V1DB.Create(name); err != nil {
		http.Error(w, fmt.Sprint(err), 400)
	}
}

func Rename(w http.ResponseWriter, r *http.Request) {
}

func Drop(w http.ResponseWriter, r *http.Request) {
}
