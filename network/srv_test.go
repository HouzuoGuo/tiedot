/* Server structure test cases. */
package network

import (
	"io/ioutil"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"
)

func TestNewServerTaskSubmit(t *testing.T) {
	wd := "/tmp/tiedot_test_srv"
	db := "/tmp/tiedot_test_db"
	os.RemoveAll(wd)
	os.RemoveAll(db)
	srvs := make([]*Server, 3)
	serversReady := &sync.WaitGroup{}
	serversReady.Add(3)
	var serverError error
	for i := 0; i < 3; i++ {
		go func(i int) {
			defer serversReady.Done()
			var err error
			if srvs[i], err = NewServer(i, 3, db, wd); err != nil {
				serverError = err
			}
		}(i)
	}
	if serverError != nil {
		t.Fatal(serverError)
	}
	serversReady.Wait()
	for i := 0; i < 3; i++ {
		srv := srvs[i]
		go srv.Start()
		if !(srv.ServerSock == "/tmp/tiedot_test_srv/"+strconv.Itoa(i) &&
			srv.Rank == i && srv.TotalRank == 3 && srv.TempDir == wd && srv.DBDir == db &&
			len(srv.ColNumParts) == 0 && len(srv.ColParts) == 0 && len(srv.Htables) == 0 && len(srv.mainLoop) == 0 && len(srv.bgLoop) == 0 &&
			len(srv.InterRank) == 3 && srv.Listener != nil) {
			t.Fatal(srv)
		}
	}
	// Foreground task
	var sideEffect bool
	promise := make(chan interface{})
	if srvs[0].submit(&Task{Ret: promise, Input: []string{"true"}, Fun: func(input []string) interface{} {
		sideEffect = input[0] == "true"
		return true
	}}).(bool) != true || !sideEffect {
		t.Fatal("wrong result")
	}
	// Background task
	var sideEffect2 bool
	srvs[1].bgLoop <- func() error {
		sideEffect2 = true
		return nil
	}
	time.Sleep(100 * time.Millisecond)
	if !sideEffect2 {
		t.Fatal("wrong result")
	}
	os.RemoveAll(wd)
	os.Remove(db)
}

func TestNewServerOpenDB(t *testing.T) {
	wd := "/tmp/tiedot_test_srv"
	db := "/tmp/tiedot_test_db"
	dirs := []string{
		// collection A of two partitions and two indexes
		"/tmp/tiedot_test_db/a",
		"/tmp/tiedot_test_db/a/chunk_0",
		"/tmp/tiedot_test_db/a/chunk_1",
		"/tmp/tiedot_test_db/a/ht_A,B,C",
		"/tmp/tiedot_test_db/a/ht_1,2,3",
		// collection B of three partitions and one index
		"/tmp/tiedot_test_db/b",
		"/tmp/tiedot_test_db/b/chunk_0",
		"/tmp/tiedot_test_db/b/chunk_1",
		"/tmp/tiedot_test_db/b/chunk_2",
		"/tmp/tiedot_test_db/b/ht_B,C,D"}
	files := []string{
		// collection A of two partitions
		"/tmp/tiedot_test_db/a/chunk_0/_data",
		"/tmp/tiedot_test_db/a/chunk_0/_pk",
		"/tmp/tiedot_test_db/a/chunk_1/_data",
		"/tmp/tiedot_test_db/a/chunk_1/_pk",
		"/tmp/tiedot_test_db/a/ht_A,B,C/0",
		"/tmp/tiedot_test_db/a/ht_A,B,C/1",
		"/tmp/tiedot_test_db/a/ht_1,2,3/0",
		"/tmp/tiedot_test_db/a/ht_1,2,3/1",
		"/tmp/tiedot_test_db/b",
		// collection B of three partitions
		"/tmp/tiedot_test_db/b/chunk_0/_data",
		"/tmp/tiedot_test_db/b/chunk_0/_pk",
		"/tmp/tiedot_test_db/b/chunk_1/_data",
		"/tmp/tiedot_test_db/b/chunk_1/_pk",
		"/tmp/tiedot_test_db/b/chunk_2/_data",
		"/tmp/tiedot_test_db/b/chunk_2/_pk",
		"/tmp/tiedot_test_db/b/ht_B,C,D/0",
		"/tmp/tiedot_test_db/b/ht_B,C,D/1",
		"/tmp/tiedot_test_db/b/ht_B,C,D/2"}
	os.RemoveAll(wd)
	os.RemoveAll(db)
	for _, dir := range dirs {
		os.MkdirAll(dir, 0700)
	}
	for _, file := range files {
		os.Create(file)
	}
	ioutil.WriteFile("/tmp/tiedot_test_db/a/numchunks", []byte("2"), 0600)
	ioutil.WriteFile("/tmp/tiedot_test_db/b/numchunks", []byte("3"), 0600)
	// Now start three servers
	srvs := make([]*Server, 3)
	serversReady := &sync.WaitGroup{}
	serversReady.Add(3)
	var serverError error
	for i := 0; i < 3; i++ {
		go func(i int) {
			defer serversReady.Done()
			var err error
			if srvs[i], err = NewServer(i, 3, db, wd); err != nil {
				serverError = err
			}
			go srvs[i].Start()
		}(i)
	}
	if serverError != nil {
		t.Fatal(serverError)
	}
	serversReady.Wait()
	// Verify server 0
	if !(srvs[0].Rank == 0 && srvs[0].TotalRank == 3 && srvs[0].TempDir == wd && srvs[0].DBDir == db &&
		len(srvs[0].ColNumParts) == 2 && len(srvs[0].ColParts) == 2 && len(srvs[0].Htables) == 2 && len(srvs[0].mainLoop) == 0 &&
		srvs[0].ColNumParts["a"] == 2 && srvs[0].ColNumParts["b"] == 3 &&
		srvs[0].ColParts["a"].BaseDir == "/tmp/tiedot_test_db/a/chunk_0" && srvs[0].ColParts["b"].BaseDir == "/tmp/tiedot_test_db/b/chunk_0" &&
		len(srvs[0].Htables["a"]) == 2 && len(srvs[0].Htables["b"]) == 1 &&
		srvs[0].Htables["a"]["A,B,C"].File.Name == "/tmp/tiedot_test_db/a/ht_A,B,C/0" && srvs[0].Htables["b"]["B,C,D"].File.Name == "/tmp/tiedot_test_db/b/ht_B,C,D/0" &&
		srvs[0].Htables["a"]["1,2,3"].File.Name == "/tmp/tiedot_test_db/a/ht_1,2,3/0" &&
		len(srvs[0].InterRank) == 3 && srvs[0].Listener != nil) {
		t.Fatal(srvs[0])
	}
	// Verify server 1
	if !(srvs[1].Rank == 1 && srvs[1].TotalRank == 3 && srvs[1].TempDir == wd && srvs[1].DBDir == db &&
		len(srvs[1].ColNumParts) == 2 && len(srvs[1].ColParts) == 2 && len(srvs[1].Htables) == 2 && len(srvs[1].mainLoop) == 0 &&
		srvs[1].ColNumParts["a"] == 2 && srvs[1].ColNumParts["b"] == 3 &&
		srvs[1].ColParts["a"].BaseDir == "/tmp/tiedot_test_db/a/chunk_1" && srvs[1].ColParts["b"].BaseDir == "/tmp/tiedot_test_db/b/chunk_1" &&
		len(srvs[1].Htables["a"]) == 2 && len(srvs[1].Htables["b"]) == 1 &&
		srvs[1].Htables["a"]["A,B,C"].File.Name == "/tmp/tiedot_test_db/a/ht_A,B,C/1" && srvs[1].Htables["b"]["B,C,D"].File.Name == "/tmp/tiedot_test_db/b/ht_B,C,D/1" &&
		srvs[1].Htables["a"]["1,2,3"].File.Name == "/tmp/tiedot_test_db/a/ht_1,2,3/1" &&
		len(srvs[1].InterRank) == 3 && srvs[1].Listener != nil) {
		t.Fatal(srvs[1])
	}
	// Verify server 2 (ONE LESS partition)
	if !(srvs[2].Rank == 2 && srvs[2].TotalRank == 3 && srvs[2].TempDir == wd && srvs[2].DBDir == db &&
		len(srvs[2].ColNumParts) == 2 && len(srvs[2].ColParts) == 1 && len(srvs[2].Htables) == 1 && len(srvs[2].mainLoop) == 0 &&
		srvs[2].ColNumParts["a"] == 2 && srvs[2].ColNumParts["b"] == 3 &&
		srvs[2].ColParts["b"].BaseDir == "/tmp/tiedot_test_db/b/chunk_2" &&
		len(srvs[2].Htables["b"]) == 1 &&
		srvs[2].Htables["b"]["B,C,D"].File.Name == "/tmp/tiedot_test_db/b/ht_B,C,D/2" &&
		len(srvs[2].InterRank) == 3 && srvs[2].Listener != nil) {
		t.Fatal(srvs[2])
	}
	os.RemoveAll(wd)
	os.Remove(db)
}
