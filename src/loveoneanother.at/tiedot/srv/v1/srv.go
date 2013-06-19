/* Bootstrap V1 server. */
package v1

import (
	"fmt"
	"log"
	"loveoneanother.at/tiedot/db"
	"net/http"
)

var V1DB *db.DB

func Start(db *db.DB, port int) {
	V1DB = db
	// collection management
	http.HandleFunc("/CREATE", Create)
	http.HandleFunc("/RENAME", Rename)
	http.HandleFunc("/DROP", Drop)
	// document management
	http.HandleFunc("/INSERT", Insert)
	http.HandleFunc("/GET", Get)
	http.HandleFunc("/UPDATE", Update)
	http.HandleFunc("/DELETE", Delete)
	// query
	http.HandleFunc("/SELECT", Select)
	http.HandleFunc("/FIND", Find)
	http.HandleFunc("/COUNT", Count)

	log.Printf("Listening on all interfaces, port %d", port)
	http.ListenAndServe(fmt.Sprintf(":%d", port), nil)
}
