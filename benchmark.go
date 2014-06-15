/*
Two benchmrk scenarios:
- Document CRUD benchmark (intend to catch performance regressions)
- Document CRUD in parallel (intend to catch concurrency related bugs)
*/
package main

import (
	"encoding/json"
	"fmt"
	"github.com/HouzuoGuo/tiedot/db"
	"math/rand"
	"os"
	"runtime"
	"strconv"
	"sync"
	"time"
)

// Whether to clean up (delete benchmark DB) after benchmark
var benchCleanup = true

// Invoke initializer, then run the function a number of times across multiple goroutines, and collect average time consumption.
func average(name string, total int, init func(), do func()) {
	numThreads := runtime.GOMAXPROCS(-1)
	wp := new(sync.WaitGroup)
	iter := float64(total)
	init()
	start := float64(time.Now().UTC().UnixNano())
	// Run function across multiple goroutines
	for i := 0; i < total; i += total / numThreads {
		wp.Add(1)
		go func() {
			defer wp.Done()
			for j := 0; j < total/numThreads; j++ {
				do()
			}
		}()
	}
	wp.Wait()
	end := float64(time.Now().UTC().UnixNano())
	fmt.Printf("%s %d: %d ns/iter, %d iter/sec\n", name, int(total), int((end-start)/iter), int(1000000000/((end-start)/iter)))
}

// Create a temporary database and collection for benchmark use.
func mkTmpDBAndCol(dbPath string, colName string) (col *db.Col) {
	os.RemoveAll(dbPath)
	tmpDB, err := db.OpenDB(dbPath)
	if err != nil {
		panic(err)
	}
	tmpCol, err := db.OpenCol(tmpDB, colName)
	if err != nil {
		panic(err)
	}
	return tmpCol
}

// Benchmark individual document operation: insert, read, query, update and delete.
func benchmark(benchSize int) {
	ids := make([]int, 0, benchSize)
	// Prepare a collection with two indexes
	tmp := "/tmp/tiedot_bench"
	if benchCleanup {
		defer os.RemoveAll(tmp)
	}
	col := mkTmpDBAndCol(tmp, "tmp")
	col.Index([]string{"a"})
	col.Index([]string{"b"})

	// Benchmark document insert
	average("insert", benchSize, func() {}, func() {
		var doc map[string]interface{}
		if err := json.Unmarshal([]byte(
			`{"a": `+strconv.Itoa(rand.Intn(benchSize))+`, "b": `+strconv.Itoa(rand.Intn(benchSize))+`,
			"more": "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed mi sem, ultrices mollis nisl quis, convallis volutpat ante. Class aptent taciti sociosqu ad litora torquent per conubia nostra, per inceptos himenaeos. Proin interdum egestas risus, imperdiet vulputate est. Cras semper risus sit amet dolor facilisis malesuada. Nunc velit augue, accumsan id facilisis ultricies, vehicula eget massa. Ut non dui eu magna egestas aliquam. Fusce in pellentesque risus. Aliquam ornare pharetra lacus in rhoncus. In eu commodo nibh. Praesent at lacinia quam. Curabitur laoreet pellentesque mollis. Maecenas mollis bibendum neque. Pellentesque semper justo ac purus auctor cursus. In egestas sodales metus sed dictum. Vivamus at elit nunc. Phasellus sit amet augue sed augue rhoncus congue. Aenean et molestie augue. Aliquam blandit lacus eu nunc rhoncus, vitae varius mauris placerat. Quisque velit urna, pretium quis dolor et, blandit sodales libero. Nulla sollicitudin est vel dolor feugiat viverra massa nunc."}`), &doc); err != nil {
			panic("json error")
		}
		if _, err := col.Insert(doc); err != nil {
			fmt.Println("Insert error", err)
		}
	})

	// Collect all document IDs and benchmark document read
	average("read", benchSize, func() {
		col.ForEachDoc(false, func(id int, _ []byte) bool {
			ids = append(ids, id)
			return true
		})
	}, func() {
		doc, err := col.Read(ids[rand.Intn(benchSize)])
		if doc == nil || err != nil {
			fmt.Println("Read error", doc, err)
		}
	})

	// Benchmark lookup query (two attributes)
	average("lookup", benchSize, func() {}, func() {
		var query interface{}
		if err := json.Unmarshal([]byte(`{"c": [{"eq": `+strconv.Itoa(rand.Intn(benchSize))+`, "in": ["a"], "limit": 1}, `+
			`{"eq": `+strconv.Itoa(rand.Intn(benchSize))+`, "in": ["b"], "limit": 1}]}`), &query); err != nil {
			panic("json error")
		}
		result := make(map[int]struct{})
		if err := db.EvalQuery(query, col, &result); err != nil {
			fmt.Println("Query error", err)
		}
	})

	// Benchmark document update
	average("update", benchSize, func() {}, func() {
		var doc map[string]interface{}
		if err := json.Unmarshal([]byte(
			`{"a": `+strconv.Itoa(rand.Intn(benchSize))+`, "b": `+strconv.Itoa(rand.Intn(benchSize))+`,
			"more": "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed mi sem, ultrices mollis nisl quis, convallis volutpat ante. Class aptent taciti sociosqu ad litora torquent per conubia nostra, per inceptos himenaeos. Proin interdum egestas risus, imperdiet vulputate est. Cras semper risus sit amet dolor facilisis malesuada. Nunc velit augue, accumsan id facilisis ultricies, vehicula eget massa. Ut non dui eu magna egestas aliquam. Fusce in pellentesque risus. Aliquam ornare pharetra lacus in rhoncus. In eu commodo nibh. Praesent at lacinia quam. Curabitur laoreet pellentesque mollis. Maecenas mollis bibendum neque. Pellentesque semper justo ac purus auctor cursus. In egestas sodales metus sed dictum. Vivamus at elit nunc. Phasellus sit amet augue sed augue rhoncus congue. Aenean et molestie augue. Aliquam blandit lacus eu nunc rhoncus, vitae varius mauris placerat. Quisque velit urna, pretium quis dolor et, blandit sodales libero. Nulla sollicitudin est vel dolor feugiat viverra massa nunc."}`), &doc); err != nil {
			panic("json error")
		}
		if err := col.Update(ids[rand.Intn(benchSize)], doc); err != nil {
			fmt.Println("Update error", err)
		}
	})

	// Benchmark document delete
	average("delete", benchSize, func() {}, func() {
		col.Delete(ids[rand.Intn(benchSize)])
	})
	col.Close()
}

// Run document operations (insert, read, query, update and delete) all at once.
func benchmark2(benchSize int) {
	docs := make([]int, 0, benchSize*2+1000)
	wp := new(sync.WaitGroup)
	numThreads := runtime.GOMAXPROCS(-1)
	wp.Add(5 * numThreads) // There are 5 goroutines: insert, read, query, update and delete

	// Prepare a collection with two indexes
	tmp := "/tmp/tiedot_bench2"
	if benchCleanup {
		defer os.RemoveAll(tmp)
	}
	col := mkTmpDBAndCol(tmp, "tmp")
	col.Index([]string{"a"})
	col.Index([]string{"b"})

	// Insert 1000 documents to make a start
	var docToInsert map[string]interface{}
	for j := 0; j < 1000; j++ {
		if err := json.Unmarshal([]byte(
			`{"a": `+strconv.Itoa(rand.Intn(benchSize))+`, "b": `+strconv.Itoa(rand.Intn(benchSize))+`,
			"more": "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed mi sem, ultrices mollis nisl quis, convallis volutpat ante. Class aptent taciti sociosqu ad litora torquent per conubia nostra, per inceptos himenaeos. Proin interdum egestas risus, imperdiet vulputate est. Cras semper risus sit amet dolor facilisis malesuada. Nunc velit augue, accumsan id facilisis ultricies, vehicula eget massa. Ut non dui eu magna egestas aliquam. Fusce in pellentesque risus. Aliquam ornare pharetra lacus in rhoncus. In eu commodo nibh. Praesent at lacinia quam. Curabitur laoreet pellentesque mollis. Maecenas mollis bibendum neque. Pellentesque semper justo ac purus auctor cursus. In egestas sodales metus sed dictum. Vivamus at elit nunc. Phasellus sit amet augue sed augue rhoncus congue. Aenean et molestie augue. Aliquam blandit lacus eu nunc rhoncus, vitae varius mauris placerat. Quisque velit urna, pretium quis dolor et, blandit sodales libero. Nulla sollicitudin est vel dolor feugiat viverra massa nunc."}`), &docToInsert); err != nil {
			panic("json error")
		}
		if newID, err := col.Insert(docToInsert); err == nil {
			docs = append(docs, newID)
		} else {
			fmt.Println("Insert error", err)
		}
	}
	start := float64(time.Now().UTC().UnixNano())

	// Insert benchSize * 2 documents
	for i := 0; i < numThreads; i++ {
		go func(i int) {
			fmt.Printf("Insert thread %d starting\n", i)
			defer wp.Done()
			var docToInsert map[string]interface{}
			for j := 0; j < benchSize/numThreads*2; j++ {
				if err := json.Unmarshal([]byte(
					`{"a": `+strconv.Itoa(rand.Intn(benchSize))+`, "b": `+strconv.Itoa(rand.Intn(benchSize))+`,
			"more": "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed mi sem, ultrices mollis nisl quis, convallis volutpat ante. Class aptent taciti sociosqu ad litora torquent per conubia nostra, per inceptos himenaeos. Proin interdum egestas risus, imperdiet vulputate est. Cras semper risus sit amet dolor facilisis malesuada. Nunc velit augue, accumsan id facilisis ultricies, vehicula eget massa. Ut non dui eu magna egestas aliquam. Fusce in pellentesque risus. Aliquam ornare pharetra lacus in rhoncus. In eu commodo nibh. Praesent at lacinia quam. Curabitur laoreet pellentesque mollis. Maecenas mollis bibendum neque. Pellentesque semper justo ac purus auctor cursus. In egestas sodales metus sed dictum. Vivamus at elit nunc. Phasellus sit amet augue sed augue rhoncus congue. Aenean et molestie augue. Aliquam blandit lacus eu nunc rhoncus, vitae varius mauris placerat. Quisque velit urna, pretium quis dolor et, blandit sodales libero. Nulla sollicitudin est vel dolor feugiat viverra massa nunc."}`), &docToInsert); err != nil {
					panic("json error")
				}
				if newID, err := col.Insert(docToInsert); err == nil {
					docs = append(docs, newID)
				} else {
					fmt.Println("Insert error", err)
				}
			}
			fmt.Printf("Insert thread %d completed\n", i)
		}(i)
	}

	// Read benchSize * 2 documents
	for i := 0; i < numThreads; i++ {
		go func(i int) {
			fmt.Printf("Read thread %d starting\n", i)
			defer wp.Done()
			for j := 0; j < benchSize/numThreads*2; j++ {
				col.Read(docs[rand.Intn(len(docs))])
			}
			fmt.Printf("Read thread %d completed\n", i)
		}(i)
	}

	// Query benchSize times (lookup on two attributes)
	for i := 0; i < numThreads; i++ {
		go func(i int) {
			fmt.Printf("Query thread %d starting\n", i)
			defer wp.Done()
			var query interface{}
			var err error
			for j := 0; j < benchSize/numThreads; j++ {
				if err = json.Unmarshal([]byte(`{"c": [{"eq": `+strconv.Itoa(rand.Intn(benchSize))+`, "in": ["a"], "limit": 1}, `+
					`{"eq": `+strconv.Itoa(rand.Intn(benchSize))+`, "in": ["b"], "limit": 1}]}`), &query); err != nil {
					panic("json error")
				}
				result := make(map[int]struct{})
				if err = db.EvalQuery(query, col, &result); err != nil {
					fmt.Println("Query error", err)
				}
			}
			fmt.Printf("Query thread %d completed\n", i)
		}(i)
	}

	// Update benchSize documents
	for i := 0; i < numThreads; i++ {
		go func(i int) {
			fmt.Printf("Update thread %d starting\n", i)
			defer wp.Done()
			var updated map[string]interface{}
			for j := 0; j < benchSize/numThreads; j++ {
				if err := json.Unmarshal([]byte(
					`{"a": `+strconv.Itoa(rand.Intn(benchSize))+`, "b": `+strconv.Itoa(rand.Intn(benchSize))+`,
			"more": "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed mi sem, ultrices mollis nisl quis, convallis volutpat ante. Class aptent taciti sociosqu ad litora torquent per conubia nostra, per inceptos himenaeos. Proin interdum egestas risus, imperdiet vulputate est. Cras semper risus sit amet dolor facilisis malesuada. Nunc velit augue, accumsan id facilisis ultricies, vehicula eget massa. Ut non dui eu magna egestas aliquam. Fusce in pellentesque risus. Aliquam ornare pharetra lacus in rhoncus. In eu commodo nibh. Praesent at lacinia quam. Curabitur laoreet pellentesque mollis. Maecenas mollis bibendum neque. Pellentesque semper justo ac purus auctor cursus. In egestas sodales metus sed dictum. Vivamus at elit nunc. Phasellus sit amet augue sed augue rhoncus congue. Aenean et molestie augue. Aliquam blandit lacus eu nunc rhoncus, vitae varius mauris placerat. Quisque velit urna, pretium quis dolor et, blandit sodales libero. Nulla sollicitudin est vel dolor feugiat viverra massa nunc."}`), &updated); err != nil {
					panic("json error")
				}
				col.Update(docs[rand.Intn(len(docs))], updated)
			}
			fmt.Printf("Update thread %d completed\n", i)
		}(i)
	}

	// Delete benchSize documents
	for i := 0; i < numThreads; i++ {
		go func(i int) {
			fmt.Printf("Delete thread %d starting\n", i)
			defer wp.Done()
			for j := 0; j < benchSize/numThreads; j++ {
				col.Delete(docs[rand.Intn(len(docs))])
			}
			fmt.Printf("Delete thread %d completed\n", i)
		}(i)
	}

	// Wait for all goroutines to finish, then print summary
	wp.Wait()
	end := float64(time.Now().UTC().UnixNano())
	fmt.Printf("Total operations %d: %d ns/iter, %d iter/sec\n", benchSize*7, int((end-start)/float64(benchSize)/7), int(1000000000/((end-start)/float64(benchSize)/7)))
}
