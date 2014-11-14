package main

import (
	jwt "github.com/dgrijalva/jwt-go"
	"io/ioutil"
	"time"
	"testing"
)

var (
	privateKey []byte //openssl genrsa -out rsa 1024
	publicKey  []byte //openssl rsa -in rsa -pubout > rsa.pub
)

func TestRsa(t *testing.T) {
	var err error
	if privateKey, err = ioutil.ReadFile("rsa"); err != nil {
		t.Fatal(err)
	}
	if publicKey, err = ioutil.ReadFile("rsa.pub"); err != nil {
		t.Fatal(err)
	}
	token := jwt.New(jwt.GetSigningMethod("RS256"))
	token.Claims["PERMISSION"] = "admin@tiedot"
	token.Claims["exp"] = time.Now().Add(time.Hour * 72).Unix()
	if ts, err := token.SignedString(privateKey); err != nil {
		t.Fatal(err)
	} else {
		check(t, ts)
	}
}

func check(t *testing.T, ts string) {
	var err error
	var token *jwt.Token
	if token, err = jwt.Parse(ts, func(ts *jwt.Token) (interface{}, error) {
		return publicKey, nil
	}); err !=nil { 
		t.Fatal(err)
	}
	if token.Valid {
		t.Log(token)
	} else {
		t.Log(token)
		t.Fail()
	}
}
