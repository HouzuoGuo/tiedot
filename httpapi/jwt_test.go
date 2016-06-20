package httpapi

import (
	"io/ioutil"
	"testing"
	"time"

	jwt "github.com/dgrijalva/jwt-go"
)

func TestJWTToken(t *testing.T) {
	var err error
	var privateKeyContent, publicKeyContent []byte
	if privateKeyContent, err = ioutil.ReadFile("jwt-test.key"); err != nil {
		t.Fatal(err)
	}
	if publicKeyContent, err = ioutil.ReadFile("jwt-test.pub"); err != nil {
		t.Fatal(err)
	}
	if privateKey, err = jwt.ParseRSAPrivateKeyFromPEM(privateKeyContent); err != nil {
		t.Fatal(err)
	}
	if publicKey, err = jwt.ParseRSAPublicKeyFromPEM(publicKeyContent); err != nil {
		t.Fatal(err)
	}
	token := jwt.New(jwt.GetSigningMethod("RS256"))
	token.Claims = jwt.MapClaims{
		"PERMISSION": "admin@tiedot",
		"exp":        time.Now().Add(time.Hour * 72).Unix(),
	}
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
