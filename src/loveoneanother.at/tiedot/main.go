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

func average(name string, total int, numThreads int, init func(), do func()) {
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

func benchmark() {
	// config
	BENCH_SIZE := 500000
	THREADS := 8
	runtime.GOMAXPROCS(THREADS)
	rand.Seed(time.Now().UTC().UnixNano())
	// prepare collection
	tmp := "/tmp/tiedot_col_bench"
	os.RemoveAll(tmp)
	defer os.RemoveAll(tmp)
	col, err := db.OpenCol(tmp)
	if err != nil {
		panic(err)
	}
	col.Index([]string{"a", "b", "c"})
	col.Index([]string{"a", "c", "d"})
	average("insert", BENCH_SIZE, THREADS, func() {}, func() {
		var jsonDoc interface{}
		if err := json.Unmarshal([]byte(
			`{"a": {"b": {"c": `+strconv.Itoa(rand.Intn(BENCH_SIZE))+`}},`+
				`"c": {"d": `+strconv.Itoa(rand.Intn(BENCH_SIZE))+`},`+
				`"more": "abcdefghijklmnopqrstuvwxyz"}`), &jsonDoc); err != nil {
			panic("json error")
		}
		if _, err := col.Insert(jsonDoc); err != nil {
			panic("insert error")
		}
	})
	ids := make([]uint64, 0)
	average("read", BENCH_SIZE, THREADS, func() {
		col.ForAll(func(id uint64, doc interface{}) bool {
			ids = append(ids, id)
			return true
		})
	}, func() {
		doc, _ := col.Read(ids[uint64(rand.Intn(BENCH_SIZE))])
		if doc == nil {
			panic("read error")
		}
	})
	average("lookup", BENCH_SIZE, THREADS, func() {}, func() {
		var query interface{}
		if err := json.Unmarshal([]byte(`["=", {"eq": `+strconv.Itoa(rand.Intn(BENCH_SIZE))+`, "in": ["a", "c", "d"]}]`), &query); err != nil {
			panic("json error")
		}
		result := make(map[uint64]bool)
		if err := db.EvalQuery(query, col, &result); err != nil {
			panic("query")
		}
	})
	average("update", BENCH_SIZE, THREADS, func() {}, func() {
		var jsonDoc interface{}
		if err := json.Unmarshal([]byte(
			`{"a": {"b": {"c": `+strconv.Itoa(rand.Int())+`}},`+
				`"c": {"d": `+strconv.Itoa(rand.Int())+`},`+
				`"more": "abcdefghijklmnopqrstuvwxyz"}`), &jsonDoc); err != nil {
			panic("json error")
		}
		if _, err := col.Update(ids[rand.Intn(BENCH_SIZE)], jsonDoc); err != nil {
			panic("update error")
		}
	})
	average("delete", BENCH_SIZE, THREADS, func() {}, func() {
		col.Delete(ids[rand.Intn(BENCH_SIZE)])
	})

	col.Close()
}

func main() {
	benchmark()
}
