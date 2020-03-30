package httpapi

import (
	"fmt"
	"math/rand"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/HouzuoGuo/tiedot/db"
)

var (
	requestShutDown    = "http://localhost:8080/shutdown"
	requestDumpNotDest = "http://localhost:8080/dump"
	requestDump        = "http://localhost:8080/dump?dest=%s"
	requestMemstats    = "http://localhost:8080/memstats"
	requestVersion     = "http://localhost:8080/version"

	listStats = []string{
		"Alloc", "TotalAlloc", "Sys",
		"Lookups", "Mallocs", "Frees",
		"HeapAlloc", "HeapSys", "HeapIdle",
		"HeapInuse", "HeapReleased", "HeapObjects",
		"StackInuse", "StackSys", "MSpanInuse",
		"MSpanSys", "MCacheInuse", "MCacheSys",
		"BuckHashSys", "GCSys", "OtherSys",
		"NextGC", "LastGC", "PauseTotalNs", "PauseNs"}
)

func TestMisc(t *testing.T) {
	testsMisc := []func(t *testing.T){
		TDumpNotDest,
		TDump,
		TDumpError,
		TMemStats,
		TVersion,
	}
	managerSubTests(testsMisc, "misc_test", t)
}
func TDumpNotDest(t *testing.T) {
	setupTestCase()
	defer tearDownTestCase()
	var err error
	if HttpDB, err = db.OpenDB(tempDir); err != nil {
		panic(err)
	}

	wDump := httptest.NewRecorder()
	reqDump := httptest.NewRequest(RandMethodRequest(), requestDumpNotDest, nil)
	Dump(wDump, reqDump)

	if wDump.Code != 400 || strings.TrimSpace(wDump.Body.String()) != "Please pass POST/PUT/GET parameter value of 'dest'." {
		t.Error("Expected code 400 and message error not such param 'dest'")
	}
}
func TDump(t *testing.T) {
	setupTestCase()
	defer tearDownTestCase()
	var err error
	var tmp2 = "./tmp2"
	if HttpDB, err = db.OpenDB(tempDir); err != nil {
		panic(err)
	}

	wDump := httptest.NewRecorder()
	reqDump := httptest.NewRequest(RandMethodRequest(), fmt.Sprintf(requestDump, tmp2), nil)

	Dump(wDump, reqDump)
	_, err = os.Stat(tmp2)

	if wDump.Code != 200 || os.IsNotExist(err) != false {
		t.Error("Expected code 200 and exist folder")
	}
	os.RemoveAll(tmp2)
}
func TDumpError(t *testing.T) {
	setupTestCase()
	defer tearDownTestCase()
	var err error

	if HttpDB, err = db.OpenDB(tempDir); err != nil {
		panic(err)
	}

	wDump := httptest.NewRecorder()
	reqDump := httptest.NewRequest(RandMethodRequest(), fmt.Sprintf(requestDump, tempDir), nil)

	Dump(wDump, reqDump)
	if wDump.Code != 500 || strings.TrimSpace(wDump.Body.String()) != "Destination file tmp/data-config.json already exists" {
		t.Error("Expected code 500 and error message folder exists.", wDump.Code, wDump.Body.String())
	}
}
func TMemStats(t *testing.T) {
	setupTestCase()
	defer tearDownTestCase()
	var err error

	if HttpDB, err = db.OpenDB(tempDir); err != nil {
		panic(err)
	}
	wMemStats := httptest.NewRecorder()
	reqMemStats := httptest.NewRequest(RandMethodRequest(), requestMemstats, nil)

	MemStats(wMemStats, reqMemStats)

	if wMemStats.Code != 200 || !strings.Contains(wMemStats.Body.String(), listStats[rand.Intn(len(listStats))]) {
		t.Error("Expected code 200 and return json with stats memory.")
	}
}
func TVersion(t *testing.T) {
	setupTestCase()
	defer tearDownTestCase()
	var err error

	if HttpDB, err = db.OpenDB(tempDir); err != nil {
		panic(err)
	}
	wVersion := httptest.NewRecorder()
	reqVersion := httptest.NewRequest(RandMethodRequest(), requestVersion, nil)
	Version(wVersion, reqVersion)

	if wVersion.Code != 200 || strings.TrimSpace(wVersion.Body.String()) != "6" {
		t.Error("Expected code 200 and return version '6'.")
	}
}
