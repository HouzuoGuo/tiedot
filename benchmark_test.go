package main

import (
	"fmt"
	"github.com/HouzuoGuo/tiedot/db"
	"math/rand"
	"os"
	"runtime"
	"strings"
	"testing"
	"time"
)

var benchTestSize = 50000

func averageTest(name string, fun func()) {
	iter := float64(benchSize)
	start := float64(time.Now().UTC().UnixNano())
	// Run function across multiple goroutines
	for i := 0; i < benchTestSize; i++ {
		fun()
	}
	end := float64(time.Now().UTC().UnixNano())
	fmt.Printf("%s %d: %d ns/iter, %d iter/sec\n", name, int(benchSize), int((end-start)/iter), int(1000000000/((end-start)/iter)))
}

// benchmark(1) written in test case style
func TestBenchmark1(t *testing.T) {
	runtime.GOMAXPROCS(runtime.NumCPU())
	rand.Seed(time.Now().UnixNano())

	ids := make([]uint64, 0, benchTestSize)
	// Prepare a collection with two indexes
	tmp := "/tmp/tiedot_test_bench1"
	defer os.RemoveAll(tmp)
	benchDB, col := mkTmpDBAndCol(tmp, "tmp")
	defer benchDB.Close()
	col.Index([]string{"nested", "nested", "str"})
	col.Index([]string{"nested", "nested", "int"})
	col.Index([]string{"nested", "nested", "float"})
	col.Index([]string{"strs"})
	col.Index([]string{"ints"})
	col.Index([]string{"floats"})

	// Benchmark document insert
	average("insert", func() {
		if _, err := col.Insert(sampleDoc()); err != nil {
			panic(err)
		}
	})

	// Collect all document IDs and benchmark document read
	col.ForEachDoc(func(id uint64, _ []byte) bool {
		ids = append(ids, id)
		return true
	})
	average("read", func() {
		doc, err := col.Read(ids[rand.Intn(benchTestSize)])
		if doc == nil || err != nil {
			panic(err)
		}
	})

	// Benchmark lookup query (two attributes)
	average("lookup", func() {
		result := make(map[uint64]struct{})
		if err := db.EvalQuery(sampleQuery(), col, &result); err != nil {
			panic(err)
		}
	})

	// Benchmark document update
	average("update", func() {
		if err := col.Update(ids[rand.Intn(benchTestSize)], sampleDoc()); err != nil && !strings.Contains(err.Error(), "locked") {
			panic(err)
		}
	})

	// Benchmark document delete
	var delCount uint64
	average("delete", func() {
		if err := col.Delete(ids[rand.Intn(benchTestSize)]); err == nil {
			delCount++
		}
	})
	if delCount < uint64(benchTestSize/2) {
		t.Fatal("Did not delete enough")
	}
}
