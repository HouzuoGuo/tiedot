package webjwt

import (
	"crypto/sha1"
	"encoding/base64"
	"fmt"
	"github.com/GeertJohan/go.rice"
	"github.com/HouzuoGuo/tiedot/db"
	"github.com/HouzuoGuo/tiedot/tdlog"
	jwt "github.com/dgrijalva/jwt-go"
	"html/template"
	"io/ioutil"
	"net/http"
	"reflect"
	"strconv"
	"time"
)

var WebCp string

func RegisterWebJwt(db *db.DB) {
	HttpDB = db
	if WebCp == "" || WebCp == "none" || WebCp == "no" || WebCp == "false" {
		tdlog.Noticef("Web control panel is disabled on your request")
		return
	}
	
	http.HandleFunc("/"+WebCp, handleWebCp)
	http.HandleFunc("/favicon.ico", func(w http.ResponseWriter, r *http.Request) {
		w.Write(rice.MustFindBox("static/img").MustBytes("favicon.ico"))
	})
	
	http.Handle("/"+WebCp+"/bower_components/", http.StripPrefix("/"+WebCp+"/bower_components/", http.FileServer(rice.MustFindBox("static/bower_components").HTTPBox())))
	http.Handle("/"+WebCp+"/html/", http.StripPrefix("/"+WebCp+"/html/", http.FileServer(rice.MustFindBox("static/html").HTTPBox())))
	http.Handle("/"+WebCp+"/css/", http.StripPrefix("/"+WebCp+"/css/", http.FileServer(rice.MustFindBox("static/css").HTTPBox())))
	http.Handle("/"+WebCp+"/js/", http.StripPrefix("/"+WebCp+"/js/", http.FileServer(rice.MustFindBox("static/js").HTTPBox())))
	tdlog.Noticef("Web control panel is accessible at /%s", WebCp)
	
	http.HandleFunc("/getJwt", getJwt)
	http.HandleFunc("/checkJwt", checkJwt)
}

func handleWebCp(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html")
	templateBox, err := rice.FindBox("static/views")
	if err != nil {
		panic(err)
	}
	templatesString, err := templateBox.String("templates.html")
	if err != nil {
		panic(err)
	}
	viewString, err := templateBox.String("index.html")
	if err != nil {
		panic(err)
	}
	tmpl, err := template.New("index").Parse(viewString)
	if err != nil {
		panic(err)
	}
	if err := tmpl.Execute(w, map[string]interface{}{
			"root": WebCp, 
			"templates": template.HTML(templatesString), 
			"login":"{{login}}",
			"submit":"{{submit}}",
			"panelSelected":"{{panelSelected}}",
			"jwt":"{{jwt}}" }); err != nil {
			panic(err)
	}
}

type security struct {
	user        string
	group       string
	roles       []string
	permissions []string
	secret      string
}

var (
	HttpDB     *db.DB
	privateKey []byte //openssl genrsa -out rsa 1024
	publicKey  []byte //openssl rsa -in rsa -pubout > rsa.pub
)

func getJwt(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "must-revalidate")
	w.Header().Set("Content-Type", "application/json")

	id := r.FormValue("id")
	docID, err := strconv.Atoi(id)
	if err != nil {
		http.Error(w, fmt.Sprintf("Invalid document ID '%v'.", id), 400)
		return
	}
	dbcol := HttpDB.Use("jwt")
	if dbcol == nil {
		http.Error(w, fmt.Sprintf("Collection '%s' does not exist.", "jwt"), 400)
		return
	}
	doc, err := dbcol.Read(docID)
	if doc == nil {
		http.Error(w, fmt.Sprintf("No such document ID %d.", docID), 404)
		return
	}

	sha := sha1.Sum([]byte(r.FormValue("password")))
	secret := base64.URLEncoding.EncodeToString(sha[:20])
	//tdlog.Noticef("%s", secret)
	if doc["secret"] != secret {
		http.Error(w, fmt.Sprint("Password invalid."), 404)
		return
	}

	//tdlog.Noticef("%v", doc)

	token := jwt.New(jwt.GetSigningMethod("RS256"))
	token.Claims["user"] = doc["user"]
	token.Claims["groups"] = doc["groups"]
	token.Claims["roles"] = doc["roles"]
	token.Claims["permissions"] = doc["permissions"]
	token.Claims["exp"] = time.Now().Add(time.Hour * 72).Unix()
	var tokenString string
	var e error
	if tokenString, e = token.SignedString(privateKey); e != nil {
		panic(e)
	}

	w.Header().Set("Authorization", "Bearer "+tokenString)
	w.WriteHeader(http.StatusOK)
	//fmt.Fprintf(w, "{\"token\": \"%s\"}", tokenString)
	//log.Printf("%s", tokenString)
}

func checkJwt(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	token, err := jwt.ParseFromRequest(r, func(token *jwt.Token) (interface{}, error) {
		return publicKey, nil
	})
	if token.Valid {
		//log.Printf("%v", token)
		//fmt.Fprintf(w, "{\"object\": %v}", token)
	} else {
		tdlog.Noticef("%v", err)
		fmt.Fprintf(w, "{\"error\": \"%s %s\"}", "JWT not valid,", err)
	}
}

func Wrap(fn http.HandlerFunc, jwtFlag bool) http.HandlerFunc {
	if jwtFlag == false {
		return fn
	}
	var e error
	if privateKey, e = ioutil.ReadFile("rsa"); e != nil {
		tdlog.Panicf("%s", e)
	}
	if publicKey, e = ioutil.ReadFile("rsa.pub"); e != nil {
		tdlog.Panicf("%s", e)
	}
	return func(w http.ResponseWriter, r *http.Request) {
		if origin := r.Header.Get("Origin"); origin != "" {
			w.Header().Set("Access-Control-Allow-Origin", origin)
		}
		w.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS, PUT, DELETE")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Content-Length, Accept-Encoding, X-CSRF-Token, Authorization")
		//w.Header().Set("Access-Control-Allow-Credentials", "true")
		//w.Header().Set("Content-Type", "application/json")
		t, _ := jwt.ParseFromRequest(r, func(token *jwt.Token) (interface{}, error) {
			return publicKey, nil
		})
		if t == nil {
			return
		}
		if !t.Valid {
			return
		}
		if test(t.Claims["groups"], "group1") &&
			test(t.Claims["roles"], "role1") &&
			test(t.Claims["permissions"], "permission1") {
			tdlog.Noticef("%v", t)
			fn(w, r)
		}
	}
}

func test(t interface{}, v string) bool {
	switch reflect.TypeOf(t).Kind() {
	case reflect.Slice:
		s := reflect.ValueOf(t)
		for i := 0; i < s.Len(); i++ {
			if s.Index(i).Interface() == v {
				return true
			}
		}
	}
	tdlog.Noticef("Test fails for %s.", v)
	return false
}
