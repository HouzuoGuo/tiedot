package main

import (
	"encoding/json"
	"fmt"
	"loveoneanother.at/tiedot/db"
	"math/rand"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Run function a number of times and calculate average time consumption per iteration.
func average(name string, total int, init func(), do func()) {
	numThreads := runtime.GOMAXPROCS(-1)
	wp := new(sync.WaitGroup)
	init()
	iter := float64(total)
	start := float64(time.Now().UTC().UnixNano())
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

// Benchmark document CRUD operations.
func benchmark(benchSize int) {
	// initialization
	rand.Seed(time.Now().UTC().UnixNano())
	// prepare benchmark data
	docs := make([]interface{}, benchSize)
	for i := range docs {
		if err := json.Unmarshal([]byte(
			`{"a": {"b": {"c": `+strconv.Itoa(rand.Intn(benchSize))+`}},`+
				`"c": {"d": `+strconv.Itoa(rand.Intn(benchSize))+`},`+
				`"more": "abcdefghijklmnopqrstuvwxyz"}`), &docs[i]); err != nil {
			panic("json error")
		}
	}
	// prepare collection
	tmp := "/tmp/tiedot_bench"
	os.RemoveAll(tmp)
	defer os.RemoveAll(tmp)
	col, err := db.OpenCol(tmp)
	if err != nil {
		panic(err)
	}
	col.Index([]string{"a", "b", "c"})
	col.Index([]string{"c", "d"})
	// start benchmarks
	average("insert", benchSize, func() {}, func() {
		if _, err := col.Insert(docs[rand.Intn(benchSize)]); err != nil {
			panic("insert error")
		}
	})
	ids := make([]uint64, 0)
	average("read", benchSize, func() {
		col.ForAll(func(id uint64, doc interface{}) bool {
			ids = append(ids, id)
			return true
		})
	}, func() {
		var doc interface{}
		col.Read(ids[rand.Intn(benchSize)], &doc)
		if doc == nil {
			panic("read error")
		}
	})
	average("lookup", benchSize, func() {}, func() {
		var query interface{}
		if err := json.Unmarshal([]byte(`{"c": [{"eq": `+strconv.Itoa(rand.Intn(benchSize))+`, "in": ["a", "b", "c"], "limit": 1}, `+
			`{"eq": `+strconv.Itoa(rand.Intn(benchSize))+`, "in": ["c", "d"], "limit": 1}]}`), &query); err != nil {
			panic("json error")
		}
		result := make(map[uint64]struct{})
		if err := db.EvalQueryV2(query, col, &result); err != nil {
			panic("query error")
		}
	})
	average("update", benchSize, func() {}, func() {
		if _, err := col.Update(ids[rand.Intn(benchSize)], docs[rand.Intn(benchSize)]); err != nil {
			panic("update error")
		}
	})
	average("delete", benchSize, func() {}, func() {
		col.Delete(ids[rand.Intn(benchSize)])
	})
	col.Close()
}

// Insert/update/delete/query all running at once.
func benchmark2(benchSize int) {
	numThreads := runtime.GOMAXPROCS(-1)
	rand.Seed(time.Now().UTC().UnixNano())
	// prepare collection
	tmp := "/tmp/tiedot_bench"
	os.RemoveAll(tmp)
	col, err := db.OpenCol(tmp)
	if err != nil {
		panic(err)
	}
	col.Index([]string{"a", "b", "c"})
	col.Index([]string{"c", "d"})
	docs := make([]uint64, 0, benchSize*2+1000)
	docsMutex := new(sync.Mutex)
	// Prepare 1000 docs as a start
	var docToInsert interface{}
	for j := 0; j < 1000; j++ {
		if err = json.Unmarshal([]byte(
			`{"a": {"b": {"c": `+strconv.Itoa(rand.Intn(benchSize))+`}},`+
				`"c": {"d": `+strconv.Itoa(rand.Intn(benchSize))+`},`+
				`"more": "abcdefghijklmnopqrstuvwxyz"}`), &docToInsert); err != nil {
			panic(err)
		}
		if newID, err := col.Insert(docToInsert); err == nil {
			docs = append(docs, newID)
		} else {
			panic(err)
		}
	}
	// benchmark begins
	wp := new(sync.WaitGroup)
	wp.Add(5 * numThreads) // (CRUD + query) * number of benchmark threads
	start := float64(time.Now().UTC().UnixNano())
	// insert benchSize * 2 documents
	for i := 0; i < numThreads; i++ {
		go func(i int) {
			fmt.Printf("Insert thread %d starting\n", i)
			defer wp.Done()
			var docToInsert interface{}
			var err error
			for j := 0; j < benchSize/numThreads*2; j++ {
				if err = json.Unmarshal([]byte(
					`{"a": {"b": {"c": `+strconv.Itoa(rand.Intn(benchSize))+`}},`+
						`"c": {"d": `+strconv.Itoa(rand.Intn(benchSize))+`},`+
						`"more": "abcdefghijklmnopqrstuvwxyz"}`), &docToInsert); err != nil {
					panic(err)
				}
				if newID, err := col.Insert(docToInsert); err == nil {
					docsMutex.Lock()
					docs = append(docs, newID)
					docsMutex.Unlock()
				} else {
					panic(err)
				}
			}
			fmt.Printf("Insert thread %d completed\n", i)
		}(i)
	}
	// read benchSize * 2 documents
	for i := 0; i < numThreads; i++ {
		go func(i int) {
			fmt.Printf("Read thread %d starting\n", i)
			defer wp.Done()
			var doc interface{}
			for j := 0; j < benchSize/numThreads*2; j++ {
				col.Read(docs[uint64(rand.Intn(len(docs)))], &doc)
			}
			fmt.Printf("Read thread %d completed\n", i)
		}(i)
	}
	// query benchSize times
	for i := 0; i < numThreads; i++ {
		go func(i int) {
			fmt.Printf("Query thread %d starting\n", i)
			defer wp.Done()
			var query interface{}
			var err error
			for j := 0; j < benchSize/numThreads; j++ {
				if err = json.Unmarshal([]byte(`{"c": [{"eq": `+strconv.Itoa(rand.Intn(benchSize))+`, "in": ["a", "b", "c"], "limit": 1}, `+
					`{"eq": `+strconv.Itoa(rand.Intn(benchSize))+`, "in": ["c", "d"], "limit": 1}]}`), &query); err != nil {
					panic("json error")
				}
				result := make(map[uint64]struct{})
				if err = db.EvalQueryV2(query, col, &result); err != nil {
					panic("query error")
				}
			}
			fmt.Printf("Query thread %d completed\n", i)
		}(i)
	}
	// update benchSize documents
	for i := 0; i < numThreads; i++ {
		go func(i int) {
			fmt.Printf("Update thread %d starting\n", i)
			defer wp.Done()
			var updated interface{}
			var err error
			for j := 0; j < benchSize/numThreads; j++ {
				if err = json.Unmarshal([]byte(
					`{"a": {"b": {"c": `+strconv.Itoa(rand.Intn(benchSize))+`}},`+
						`"c": {"d": `+strconv.Itoa(rand.Intn(benchSize))+`},`+
						`"more": "abcdefghijklmnopqrstuvwxyz"}`), &updated); err != nil {
					panic(err)
				}
				if _, err = col.Update(docs[uint64(rand.Intn(len(docs)))], updated); err != nil {
					// "does not exist" indicates that a deleted document is being updated, it is safe to ignore
					if !strings.Contains(fmt.Sprint(err), "does not exist") {
						fmt.Println(err)
					}
				}
			}
			fmt.Printf("Update thread %d completed\n", i)
		}(i)
	}
	// delete benchSize documents
	for i := 0; i < numThreads; i++ {
		go func(i int) {
			fmt.Printf("Delete thread %d starting\n", i)
			defer wp.Done()
			for j := 0; j < benchSize/numThreads; j++ {
				col.Delete(docs[uint64(rand.Intn(len(docs)))])
			}
			fmt.Printf("Delete thread %d completed\n", i)
		}(i)
	}
	wp.Wait()
	end := float64(time.Now().UTC().UnixNano())
	fmt.Printf("Total operations %d: %d ns/iter, %d iter/sec\n", benchSize*7, int((end-start)/float64(benchSize)/7), int(1000000000/((end-start)/float64(benchSize)/7)))
}

// Benchmark document operations involving UID.
func benchmark3(benchSize int) {
	// initialization
	rand.Seed(time.Now().UTC().UnixNano())
	// prepare benchmark data
	docs := make([]interface{}, benchSize)
	for i := range docs {
		if err := json.Unmarshal([]byte(
			`{"a": {"b": {"c": `+strconv.Itoa(rand.Intn(benchSize))+`}},`+
				`"c": {"d": `+strconv.Itoa(rand.Intn(benchSize))+`},`+
				`"more": "abcdefghijklmnopqrstuvwxyz"}`), &docs[i]); err != nil {
			panic("json error")
		}
	}
	// prepare collection
	tmp := "/tmp/tiedot_bench"
	os.RemoveAll(tmp)
	defer os.RemoveAll(tmp)
	col, err := db.OpenCol(tmp)
	if err != nil {
		panic(err)
	}
	col.Index([]string{"a", "b", "c"})
	col.Index([]string{"c", "d"})
	uidsMutex := new(sync.Mutex)
	uids := make([]string, 0, benchSize)
	// start benchmarks
	average("insert", benchSize, func() {}, func() {
		if _, uid, err := col.InsertWithUID(docs[rand.Intn(benchSize)]); err == nil {
			uidsMutex.Lock()
			uids = append(uids, uid)
			uidsMutex.Unlock()
		} else {
			panic("insert error")
		}
	})

	average("read", benchSize, func() {
	}, func() {
		var doc interface{}
		col.ReadByUID(uids[rand.Intn(benchSize)], &doc)
	})
	average("lookup", benchSize, func() {}, func() {
		var query interface{}
		if err := json.Unmarshal([]byte(`{"c": [{"eq": `+strconv.Itoa(rand.Intn(benchSize))+`, "in": ["a", "b", "c"], "limit": 1}, `+
			`{"eq": `+strconv.Itoa(rand.Intn(benchSize))+`, "in": ["c", "d"], "limit": 1}]}`), &query); err != nil {
			panic("json error")
		}
		result := make(map[uint64]struct{})
		if err := db.EvalQueryV2(query, col, &result); err != nil {
			panic("query error")
		}
	})
	average("update", benchSize, func() {}, func() {
		col.UpdateByUID(uids[rand.Intn(benchSize)], docs[rand.Intn(benchSize)])
	})
	average("delete", benchSize, func() {}, func() {
		col.DeleteByUID(uids[rand.Intn(benchSize)])
	})
	col.Close()
}
