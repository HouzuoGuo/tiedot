/* Bootstrap V1 server. */
package v1

import (
	"encoding/json"
	"fmt"
	"log"
	"loveoneanother.at/tiedot/db"
	"net/http"
)

var V1DB *db.DB

// Put form parameter value of specified key to *val, return false and set HTTP error status if the parameter is not set.
func Require(w http.ResponseWriter, r *http.Request, key string, val *string) bool {
	*val = r.FormValue(key)
	if *val == "" {
		http.Error(w, fmt.Sprintf("Please pass POST/PUT/GET parameter value of '%s'", key), 400)
		return false
	}
	return true
}

// Common server response.
func Respond(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Cache-Control", "must-revalidate")
	w.WriteHeader(status)
	if data != nil {
		if response, err := json.Marshal(data); err != nil {
			log.Printf("Cannot serialize server response '%v' to JSON", data)
		} else {
			w.Write(response)
		}
	}
}

func Start(db *db.DB, port int) {
	V1DB = db
	// collection management
	http.HandleFunc("/create", Create)
	http.HandleFunc("/rename", Rename)
	http.HandleFunc("/drop", Drop)
	http.HandleFunc("/all", All)
	// document management
	http.HandleFunc("/insert", Insert)
	http.HandleFunc("/get", Get)
	http.HandleFunc("/update", Update)
	http.HandleFunc("/delete", Delete)
	// query
	http.HandleFunc("/select", Select)
	http.HandleFunc("/find", Find)
	http.HandleFunc("/count", Count)

	log.Printf("Listening on all interfaces, port %d", port)
	http.ListenAndServe(fmt.Sprintf(":%d", port), nil)
}
