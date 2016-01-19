package main

import (
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/HouzuoGuo/tiedot/db"
)

var benchTestSize = 100000

func averageTest(name string, fun func()) {
	numThreads := runtime.GOMAXPROCS(-1)
	wp := new(sync.WaitGroup)
	iter := float64(benchTestSize)
	start := float64(time.Now().UTC().UnixNano())
	// Run function across multiple goroutines
	for i := 0; i < benchTestSize; i += benchTestSize / numThreads {
		wp.Add(1)
		go func() {
			defer wp.Done()
			for j := 0; j < benchTestSize/numThreads; j++ {
				fun()
			}
		}()
	}
	wp.Wait()
	end := float64(time.Now().UTC().UnixNano())
	fmt.Printf("%s %d: %d ns/iter, %d iter/sec\n", name, int(benchTestSize), int((end-start)/iter), int(1000000000/((end-start)/iter)))
}

// benchmark(1) written in test case style
func TestBenchmark1(t *testing.T) {
	return
	runtime.GOMAXPROCS(runtime.NumCPU())
	rand.Seed(time.Now().UnixNano())

	ids := make([]int, 0, benchTestSize)
	// Prepare a collection with two indexes
	tmp := "/tmp/tiedot_test_bench1"
	os.RemoveAll(tmp)
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
	col.ForEachDoc(func(id int, _ []byte) bool {
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
		result := make(map[int]struct{})
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
	var delCount int64
	average("delete", func() {
		if err := col.Delete(ids[rand.Intn(benchTestSize)]); err == nil {
			atomic.AddInt64(&delCount, 1)
		}
	})
	if delCount < int64(benchTestSize/2) {
		t.Fatal("Did not delete enough")
	}
}

// benchmark2 written in test case style
func TestBenchmark2(t *testing.T) {
	return
	runtime.GOMAXPROCS(runtime.NumCPU())
	rand.Seed(time.Now().UnixNano())

	docs := make([]int, 0, benchTestSize*2+1000)
	wp := new(sync.WaitGroup)
	numThreads := runtime.GOMAXPROCS(-1)
	// There are goroutines doing document operations: insert, read, query, update, delete
	wp.Add(5 * numThreads)
	// And one more changing schema and stuff
	wp.Add(1)

	// Prepare a collection with two indexes
	tmp := "/tmp/tiedot_test_bench2"
	os.RemoveAll(tmp)
	defer os.RemoveAll(tmp)
	benchdb, col := mkTmpDBAndCol(tmp, "tmp")
	defer benchdb.Close()
	col.Index([]string{"nested", "nested", "str"})
	col.Index([]string{"nested", "nested", "int"})
	col.Index([]string{"nested", "nested", "float"})
	col.Index([]string{"strs"})
	col.Index([]string{"ints"})
	col.Index([]string{"floats"})

	// Insert 1000 documents to make a start
	for j := 0; j < 1000; j++ {
		if newID, err := col.Insert(sampleDoc()); err == nil {
			docs = append(docs, newID)
		} else {
			panic(err)
		}
	}
	start := float64(time.Now().UTC().UnixNano())

	// Insert benchTestSize * 2 documents
	for i := 0; i < numThreads; i++ {
		go func(i int) {
			fmt.Printf("Insert thread %d starting\n", i)
			defer wp.Done()
			for j := 0; j < benchTestSize/numThreads*2; j++ {
				if newID, err := col.Insert(sampleDoc()); err == nil {
					docs = append(docs, newID)
				} else {
					panic(err)
				}
			}
			fmt.Printf("Insert thread %d completed\n", i)
		}(i)
	}

	// Read benchTestSize * 2 documents
	var readCount int64
	for i := 0; i < numThreads; i++ {
		go func(i int) {
			fmt.Printf("Read thread %d starting\n", i)
			defer wp.Done()
			for j := 0; j < benchTestSize/numThreads*2; j++ {
				if _, err := col.Read(docs[rand.Intn(len(docs))]); err == nil {
					atomic.AddInt64(&readCount, 1)
				}
			}
			fmt.Printf("Read thread %d completed\n", i)
		}(i)
	}

	// Query benchTestSize times (lookup on two attributes)
	for i := 0; i < numThreads; i++ {
		go func(i int) {
			fmt.Printf("Query thread %d starting\n", i)
			defer wp.Done()
			var err error
			for j := 0; j < benchTestSize/numThreads; j++ {
				result := make(map[int]struct{})
				if err = db.EvalQuery(sampleQuery(), col, &result); err != nil {
					panic(err)
				}
			}
			fmt.Printf("Query thread %d completed\n", i)
		}(i)
	}

	// Update benchTestSize documents
	var updateCount int64
	for i := 0; i < numThreads; i++ {
		go func(i int) {
			fmt.Printf("Update thread %d starting\n", i)
			defer wp.Done()
			for j := 0; j < benchTestSize/numThreads; j++ {
				if err := col.Update(docs[rand.Intn(len(docs))], sampleDoc()); err == nil {
					atomic.AddInt64(&updateCount, 1)
				}
			}
			fmt.Printf("Update thread %d completed\n", i)
		}(i)
	}

	// Delete benchTestSize documents
	var delCount int64
	for i := 0; i < numThreads; i++ {
		go func(i int) {
			fmt.Printf("Delete thread %d starting\n", i)
			defer wp.Done()
			for j := 0; j < benchTestSize/numThreads; j++ {
				if err := col.Delete(docs[rand.Intn(len(docs))]); err == nil {
					atomic.AddInt64(&delCount, 1)
				}
			}
			fmt.Printf("Delete thread %d completed\n", i)
		}(i)
	}

	// This one does a bunch of schema-changing stuff, testing the engine while document operations are busy
	go func() {
		time.Sleep(500 * time.Millisecond)
		if err := benchdb.Create("foo"); err != nil {
			panic(err)
		} else if err := benchdb.Rename("foo", "bar"); err != nil {
			panic(err)
		} else if err := benchdb.Truncate("bar"); err != nil {
			panic(err)
		} else if err := benchdb.Scrub("bar"); err != nil {
			panic(err)
		} else if benchdb.Use("bar") == nil {
			panic("Missing collection")
		}
		for _, colName := range benchdb.AllCols() {
			if colName != "bar" && colName != "tmp" {
				panic("Wrong collections in benchmark db")
			}
		}
		os.RemoveAll("/tmp/tiedot_test_bench2_dump")
		defer os.RemoveAll("/tmp/tiedot_test_bench2_dump")
		if err := benchdb.Dump("/tmp/tiedot_test_bench2_dump"); err != nil {
			panic(err)
		} else if err := benchdb.Drop("bar"); err != nil {
			panic(err)
		}
		defer wp.Done()
	}()

	// Wait for all goroutines to finish, then print summary
	wp.Wait()
	end := float64(time.Now().UTC().UnixNano())
	fmt.Printf("Total operations %d: %d ns/iter, %d iter/sec\n", benchTestSize*7, int((end-start)/float64(benchTestSize)/7), int(1000000000/((end-start)/float64(benchTestSize)/7)))
	fmt.Printf("Read %d documents\n", readCount)
	fmt.Printf("Updated %d documents\n", updateCount)
	fmt.Printf("Deleted %d documents\n", delCount)
	if readCount < int64(benchTestSize/3) {
		t.Fatal("Did not read enough documents")
	}
	if updateCount < int64(benchTestSize/8) {
		t.Fatal("Did not update enough documents")
	}
	if delCount < int64(benchTestSize/8) {
		t.Fatal("Did not delete enough documents")
	}
}
