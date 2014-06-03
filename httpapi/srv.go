/* Bootstrap V3 server. */
package httpapi

import (
	"fmt"
	"github.com/GeertJohan/go.rice"
	"github.com/HouzuoGuo/tiedot/db"
	"github.com/HouzuoGuo/tiedot/tdlog"
	"html/template"
	"net/http"
	"sync"
)

var HttpDB *db.DB
var HttpDBSync = new(sync.RWMutex) // To synchronize "stop-the-world" operations
var WebCp string

// Store form parameter value of specified key to *val and return true; if key does not exist, set HTTP status 400 and return false.
func Require(w http.ResponseWriter, r *http.Request, key string, val *string) bool {
	*val = r.FormValue(key)
	if *val == "" {
		http.Error(w, fmt.Sprintf("Please pass POST/PUT/GET parameter value of '%s'.", key), 400)
		return false
	}
	return true
}

func Start(db *db.DB, port int, webcp string) {
	HttpDB = db
	WebCp = webcp

	// collection management (stop-the-world)
	http.HandleFunc("/create", Create)
	http.HandleFunc("/rename", Rename)
	http.HandleFunc("/drop", Drop)
	http.HandleFunc("/all", All)
	http.HandleFunc("/scrub", Scrub)
	http.HandleFunc("/sync", Sync)
	// query
	http.HandleFunc("/query", Query)
	http.HandleFunc("/count", Count)
	// document management
	http.HandleFunc("/insert", Insert)
	http.HandleFunc("/get", Get)
	http.HandleFunc("/update", Update)
	http.HandleFunc("/delete", Delete)
	// index management (stop-the-world)
	http.HandleFunc("/index", Index)
	http.HandleFunc("/indexes", Indexes)
	http.HandleFunc("/unindex", Unindex)
	// misc (stop-the-world)
	http.HandleFunc("/shutdown", Shutdown)
	http.HandleFunc("/dump", Dump)
	// misc
	http.HandleFunc("/version", Version)
	http.HandleFunc("/memstats", MemStats)

	//web control panel
	if WebCp != "" {
		http.HandleFunc("/"+WebCp, HandleWebcp)
		http.Handle("/"+WebCp+"/css/", http.StripPrefix("/"+WebCp+"/css/", http.FileServer(rice.MustFindBox("../admin/css").HTTPBox())))
		http.Handle("/"+WebCp+"/js/", http.StripPrefix("/"+WebCp+"/js/", http.FileServer(rice.MustFindBox("../admin/js").HTTPBox())))
		tdlog.Printf("Web control panel enabled at %s", WebCp)
	}

	tdlog.Printf("Listening on all interfaces, port %d", port)
	http.ListenAndServe(fmt.Sprintf(":%d", port), nil)
}

func HandleWebcp(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html")

	templateBox, err := rice.FindBox("../admin/views")
	if err != nil {
		tdlog.Fatal(err)
	}
	templateString, err := templateBox.String("index.html")
	if err != nil {
		tdlog.Fatal(err)
	}

	tmpl, _ := template.New("index").Parse(templateString)
	tmpl.Execute(w, map[string]interface{}{"root": WebCp})
}
