package tdlog

import (
	"bytes"
	"fmt"
	"log"
	"math/rand"
	"strings"
	"testing"
)

func RandStringBytes(n int) string {
	const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
}
func TestInfof(t *testing.T) {
	VerboseLog = true
	var str bytes.Buffer
	log.SetOutput(&str)
	Infof("test %s", "argument")

	if !strings.Contains(str.String(), "test argument") {
		t.Error("Expected error not equal string from log")
	}
}
func TestInfo(t *testing.T) {
	VerboseLog = true
	var str bytes.Buffer
	log.SetOutput(&str)
	Info("test argument")

	if !strings.Contains(str.String(), "test argument") {
		t.Error("Expected error not equal string from log")
	}
}
func TestCritNoRepeatMoreLimit(t *testing.T) {
	VerboseLog = true
	for len(critHistory) < 100 {
		critHistory[RandStringBytes(5)] = struct{}{}
	}
	fmt.Println(len(critHistory))
	fmt.Println(critHistory)
	var str bytes.Buffer
	log.SetOutput(&str)
	CritNoRepeat("test %s", "argument")
}
func TestAllLogLevels(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("Did not catch Panicf")
		}
	}()
	Infof("a %s %s", "b", "c")
	Info("a", "b", "c")
	Noticef("a %s %s", "b", "c")
	Notice("a", "b", "c")
	CritNoRepeat("a %s %s", "b", "c")
	if _, exists := critHistory["a b c"]; !exists {
		t.Fatal("did not record history")
	}
	Panicf("a %s %s", "b", "c")
	t.Fatal("Cannot reach here")
}
