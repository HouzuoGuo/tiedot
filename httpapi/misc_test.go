package httpapi

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/HouzuoGuo/tiedot/db"
	"github.com/bouk/monkey"
	"math/rand"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
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
		TShutdown,
		TDumpNotDest,
		TDump,
		TDumpError,
		TMemStats,
		TVersion,
		TMemStatsErrJsonMarshal,
	}
	managerSubTests(testsMisc, "misc_test", t)
}
func TShutdown(t *testing.T) {
	var execute = false
	patch := monkey.Patch(os.Exit, func(int) {
		execute = true
	})
	defer patch.Unpatch()
	setupTestCase()
	defer tearDownTestCase()
	var err error
	if HttpDB, err = db.OpenDB(tempDir); err != nil {
		panic(err)
	}

	wShutdown := httptest.NewRecorder()
	reqShutdown := httptest.NewRequest(RandMethodRequest(), requestShutDown, nil)
	Shutdown(wShutdown, reqShutdown)

	if !execute {
		t.Error("Expected true execute os.Exit")
	}
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
	if wDump.Code != 500 || strings.TrimSpace(wDump.Body.String()) != "Destination file tmp/number_of_partitions already exists" {
		t.Error("Expected code 500 and error message folder exists.")
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
func TMemStatsErrJsonMarshal(t *testing.T) {
	setupTestCase()
	defer tearDownTestCase()
	var err error

	if HttpDB, err = db.OpenDB(tempDir); err != nil {
		panic(err)
	}
	wMemStats := httptest.NewRecorder()
	reqMemStats := httptest.NewRequest(RandMethodRequest(), requestMemstats, nil)
	var textError = "Error json marshal"
	patch := monkey.Patch(json.Marshal, func(interface{}) ([]byte, error) {
		return nil, errors.New(textError)
	})
	defer patch.Unpatch()
	MemStats(wMemStats, reqMemStats)

	if wMemStats.Code != 500 || strings.TrimSpace(wMemStats.Body.String()) != "Cannot serialize MemStats to JSON." {
		t.Error("Expected code 500 and message error serialize json.")
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
