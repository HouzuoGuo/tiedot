package main

import (
	jwt "github.com/dgrijalva/jwt-go"
	"io/ioutil"
	"testing"
	"time"
)

var (
	// openssl req -x509 -nodes -days 365 -newkey rsa:2048 -keyout rsa-test.key -out rsa-test.pub
	privateKey []byte
	publicKey  []byte
)

func TestJWTToken(t *testing.T) {
	var err error
	if privateKey, err = ioutil.ReadFile("rsa-test.key"); err != nil {
		t.Fatal(err)
	}
	if publicKey, err = ioutil.ReadFile("rsa-test.pub"); err != nil {
		t.Fatal(err)
	}
	token := jwt.New(jwt.GetSigningMethod("RS256"))
	token.Claims["PERMISSION"] = "admin@tiedot"
	token.Claims["exp"] = time.Now().Add(time.Hour * 72).Unix()
	ts, err := token.SignedString(privateKey)
	if err != nil {
		t.Fatal(err)
	}
	if token, err = jwt.Parse(ts, func(ts *jwt.Token) (interface{}, error) {
		return publicKey, nil
	}); err != nil {
		t.Fatal(err)
	}
	if token.Valid {
		t.Log(token)
	} else {
		t.Log(token)
		t.Fail()
	}
}
