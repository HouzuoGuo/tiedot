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

func All(w http.ResponseWriter, r *http.Request) {
}

func Rename(w http.ResponseWriter, r *http.Request) {
	var oldName, newName string
	if !Require(w, r, "old", &oldName) {
		return
	}
	if !Require(w, r, "new", &newName) {
		return
	}
	if err := V1DB.Rename(oldName, newName); err != nil {
		http.Error(w, fmt.Sprint(err), 400)
	}
}

func Drop(w http.ResponseWriter, r *http.Request) {
}
