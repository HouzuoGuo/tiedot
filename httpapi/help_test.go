package httpapi

import (
	"log"
	"math/rand"
	"os"
	"testing"
)

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

// setUp and tearDown
func setupTestCase() {
	if err := os.MkdirAll(tempDir, 0700); err != nil {
		log.Println(err)
	}

}
func tearDownTestCase() {
	os.RemoveAll(tempDir)
}

func RandStringBytes(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
}
func RandMethodRequest() string {
	methods := []string{"GET", "POST", "OPTIONS", "PUT"}
	return methods[rand.Intn(len(methods))]
}

func managerSubTests(tests []func(t *testing.T), nameGroup string, t *testing.T) {
	for _, tc := range tests {
		tc := tc // capture range variable
		t.Run(nameGroup, tc)
	}
}
