/*
{
    "collections": [
        "jwt",
        "test"
    ],
    "paths": [
        "all",
        "update"
    ],
    "secret": "2jmj7l5rSw0yVb_vlWAYkK_YBwk=",
    "user": "admin"
}
*/

package httpapi

import (
	"crypto/sha1"
	"encoding/base64"
	"fmt"
	"github.com/HouzuoGuo/tiedot/tdlog"
	jwt "github.com/dgrijalva/jwt-go"
	"io/ioutil"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"time"
)

var (
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
	//tdlog.Notice(secret)
	if doc["secret"] != secret {
		http.Error(w, fmt.Sprint("Password invalid."), 404)
		return
	}

	//tdlog.Notice(doc)

	token := jwt.New(jwt.GetSigningMethod("RS256"))
	token.Claims["user"] = doc["user"]
	token.Claims["collections"] = doc["collections"]
	token.Claims["paths"] = doc["paths"]
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

func wrap(fn http.HandlerFunc, jwtFlag bool) http.HandlerFunc {
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
		if t.Claims["user"] == "admin" {
			fn(w, r)
			return
		}

		var url = strings.TrimPrefix(r.URL.Path, "/")
		var col = r.FormValue("col")

		if test(t.Claims["paths"], url) &&
			test(t.Claims["collections"], col) {
			fn(w, r)
			//tdlog.Notice(url, " ", col)
			//tdlog.Notice(t)
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
