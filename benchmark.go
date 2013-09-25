package main

import (
	"encoding/json"
	"fmt"
	"loveoneanother.at/tiedot/db"
	"math/rand"
	"os"
	"runtime"
	"strconv"
	"sync"
	"time"
)

const BENCH_SIZE = 400000 // don't make it too large... unmarshaled JSON takes lots of memory!

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

// Individual feature benchmarks.
func benchmark() {
	// initialization
	rand.Seed(time.Now().UTC().UnixNano())
	// prepare benchmark data
	docs := [BENCH_SIZE]interface{}{}
	for i := range docs {
		if err := json.Unmarshal([]byte(
			`{"a": {"b": {"c": `+strconv.Itoa(rand.Intn(BENCH_SIZE))+`}},`+
				`"c": {"d": `+strconv.Itoa(rand.Intn(BENCH_SIZE))+`},`+
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
	average("insert", BENCH_SIZE, func() {}, func() {
		if _, err := col.Insert(docs[rand.Intn(BENCH_SIZE)]); err != nil {
			panic("insert error")
		}
	})
	ids := make([]uint64, 0)
	average("read", BENCH_SIZE, func() {
		col.ForAll(func(id uint64, doc interface{}) bool {
			ids = append(ids, id)
			return true
		})
	}, func() {
		var doc interface{}
		err = col.Read(ids[uint64(rand.Intn(BENCH_SIZE))], &doc)
		if doc == nil {
			panic("read error")
		}
	})
	average("lookup", BENCH_SIZE, func() {}, func() {
		var query interface{}
		if err := json.Unmarshal([]byte(`{"c": [{"eq": `+strconv.Itoa(rand.Intn(BENCH_SIZE))+`, "in": ["a", "b", "c"], "limit": 1}, `+
			`{"eq": `+strconv.Itoa(rand.Intn(BENCH_SIZE))+`, "in": ["c", "d"], "limit": 1}]}`), &query); err != nil {
			panic("json error")
		}
		result := make(map[uint64]struct{})
		if err := db.EvalQueryV2(query, col, &result); err != nil {
			panic("query error")
		}
	})
	average("update", BENCH_SIZE, func() {}, func() {
		if _, err := col.Update(ids[rand.Intn(BENCH_SIZE)], docs[rand.Intn(BENCH_SIZE)]); err != nil {
			panic("update error")
		}
	})
	average("delete", BENCH_SIZE, func() {}, func() {
		col.Delete(ids[rand.Intn(BENCH_SIZE)])
	})
	col.Close()
}

// Insert/update/delete/query all running at once.
func benchmark2() {
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
	docs := make([]uint64, 0, BENCH_SIZE)
	// Prepare 1000 docs as a start
	var docToInsert interface{}
	for j := 0; j < 1000; j++ {
		if err = json.Unmarshal([]byte(
			`{"a": {"b": {"c": `+strconv.Itoa(rand.Intn(BENCH_SIZE))+`}},`+
				`"c": {"d": `+strconv.Itoa(rand.Intn(BENCH_SIZE))+`},`+
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
	// insert BENCH_SIZE * 2 documents
	for i := 0; i < numThreads; i++ {
		go func() {
			defer wp.Done()
			var docToInsert interface{}
			var err error
			for j := 0; j < BENCH_SIZE/numThreads*2; j++ {
				if err = json.Unmarshal([]byte(
					`{"a": {"b": {"c": `+strconv.Itoa(rand.Intn(BENCH_SIZE))+`}},`+
						`"c": {"d": `+strconv.Itoa(rand.Intn(BENCH_SIZE))+`},`+
						`"more": "abcdefghijklmnopqrstuvwxyz"}`), &docToInsert); err != nil {
					panic(err)
				}
				if newID, err := col.Insert(docToInsert); err == nil {
					docs = append(docs, newID)
				} else {
					panic(err)
				}
			}
		}()
	}
	// read BENCH_SIZE * 2 documents
	for i := 0; i < numThreads; i++ {
		go func() {
			defer wp.Done()
			var doc interface{}
			for j := 0; j < BENCH_SIZE/numThreads*2; j++ {
				col.Read(docs[uint64(rand.Intn(len(docs)))], &doc)
			}
		}()
	}
	// query BENCH_SIZE times
	for i := 0; i < numThreads; i++ {
		go func() {
			defer wp.Done()
			var query interface{}
			var err error
			for j := 0; j < BENCH_SIZE/numThreads; j++ {
				if err = json.Unmarshal([]byte(`{"c": [{"eq": `+strconv.Itoa(rand.Intn(BENCH_SIZE))+`, "in": ["a", "b", "c"], "limit": 1}, `+
					`{"eq": `+strconv.Itoa(rand.Intn(BENCH_SIZE))+`, "in": ["c", "d"], "limit": 1}]}`), &query); err != nil {
					panic("json error")
				}
				result := make(map[uint64]struct{})
				if err = db.EvalQueryV2(query, col, &result); err != nil {
					panic("query error")
				}
			}
		}()
	}
	// update BENCH_SIZE documents
	for i := 0; i < numThreads; i++ {
		go func() {
			defer wp.Done()
			var updated interface{}
			var err error
			for j := 0; j < BENCH_SIZE/numThreads; j++ {
				if err = json.Unmarshal([]byte(
					`{"a": {"b": {"c": `+strconv.Itoa(rand.Intn(BENCH_SIZE))+`}},`+
						`"c": {"d": `+strconv.Itoa(rand.Intn(BENCH_SIZE))+`},`+
						`"more": "abcdefghijklmnopqrstuvwxyz"}`), &updated); err != nil {
					panic(err)
				}
				col.Update(docs[uint64(rand.Intn(len(docs)))], updated)
			}
		}()
	}
	// delete BENCH_SIZE documents
	for i := 0; i < numThreads; i++ {
		go func() {
			defer wp.Done()
			for j := 0; j < BENCH_SIZE/numThreads; j++ {
				col.Delete(docs[uint64(rand.Intn(len(docs)))])
			}
		}()
	}
	wp.Wait()
	end := float64(time.Now().UTC().UnixNano())
	fmt.Printf("Total operations %d: %d ns/iter, %d iter/sec\n", BENCH_SIZE*7, int((end-start)/BENCH_SIZE/7), int(1000000000/((end-start)/BENCH_SIZE/7)))
}
