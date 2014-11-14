package main

import (
	"fmt"
	jwt "github.com/dgrijalva/jwt-go"
	"io/ioutil"
	"log"
	"time"
	"testing"
)

var (
	privateKey []byte //openssl genrsa -out jwt.rsa 1024
	publicKey  []byte //openssl rsa -in jwt.rsa -pubout > jwt.rsa.pub
)

init {
	if privateKey, e = ioutil.ReadFile("webjwt/jwt.rsa"); e != nil {
		tdlog.Panicf("%s", e)
	}
	if publicKey, e = ioutil.ReadFile("webjwt/jwt.rsa.pub"); e != nil {
		tdlog.Panicf("%s", e)
	}
}

func getJwtTest() {
	token := jwt.New(jwt.GetSigningMethod("RS256"))
	token.Claims["PERMISSION"] = "admin@tiedot"
	token.Claims["exp"] = time.Now().Add(time.Hour * 72).Unix()
	if tokenString, e := token.SignedString(privateKey); e != nil {
		panic(e)
	} else {
		checkJwtTest(tokenString)
	}
}

func checkJwtTest(t string) {
	token, err := jwt.Parse(t, func(token *jwt.Token) (interface{}, error) {
		return publicKey, nil
	})
	if token.Valid {
		log.Printf("%v", token)
	} else {
		log.Printf("%s", err)
	}
}

func TestAll(t *testing.T) {

}