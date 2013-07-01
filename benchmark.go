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

// Feature benchmarks.
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
		if err := json.Unmarshal([]byte(`["c", ["=", {"eq": `+strconv.Itoa(rand.Intn(BENCH_SIZE))+`, "in": ["a", "b", "c"], "limit": 1}],`+
			`["=", {"eq": `+strconv.Itoa(rand.Intn(BENCH_SIZE))+`, "in": ["c", "d"], "limit": 1}]]`), &query); err != nil {
			panic("json error")
		}
		result := make(map[uint64]bool)
		if err := db.EvalQuery(query, col, &result); err != nil {
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
