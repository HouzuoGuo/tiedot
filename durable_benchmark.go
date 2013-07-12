package main

import (
	"encoding/json"
	"loveoneanother.at/tiedot/db"
	"math/rand"
	"os"
	"strconv"
	"time"
)

const DURABLE_BENCH_SIZE = 500

// Benchmark durable operations.
func durableBenchmark() {
	// initialization
	rand.Seed(time.Now().UTC().UnixNano())
	// prepare benchmark data
	docs := [DURABLE_BENCH_SIZE]interface{}{}
	for i := range docs {
		if err := json.Unmarshal([]byte(
			`{"a": {"b": {"c": `+strconv.Itoa(rand.Intn(DURABLE_BENCH_SIZE))+`}},`+
				`"c": {"d": `+strconv.Itoa(rand.Intn(DURABLE_BENCH_SIZE))+`},`+
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
	average("insert", DURABLE_BENCH_SIZE, func() {}, func() {
		if _, err := col.DurableInsert(docs[rand.Intn(DURABLE_BENCH_SIZE)]); err != nil {
			panic("insert error")
		}
	})
	ids := make([]uint64, 0)
	average("read", DURABLE_BENCH_SIZE, func() {
		col.ForAll(func(id uint64, doc interface{}) bool {
			ids = append(ids, id)
			return true
		})
	}, func() {
		var doc interface{}
		err = col.Read(ids[uint64(rand.Intn(DURABLE_BENCH_SIZE))], &doc)
		if doc == nil {
			panic("read error")
		}
	})
	average("lookup", DURABLE_BENCH_SIZE, func() {}, func() {
		var query interface{}
		if err := json.Unmarshal([]byte(`["c", ["=", {"eq": `+strconv.Itoa(rand.Intn(DURABLE_BENCH_SIZE))+`, "in": ["a", "b", "c"], "limit": 1}],`+
			`["=", {"eq": `+strconv.Itoa(rand.Intn(DURABLE_BENCH_SIZE))+`, "in": ["c", "d"], "limit": 1}]]`), &query); err != nil {
			panic("json error")
		}
		result := make(map[uint64]struct{})
		if err := db.EvalQuery(query, col, &result); err != nil {
			panic("query error")
		}
	})
	average("update", DURABLE_BENCH_SIZE, func() {}, func() {
		if _, err := col.DurableUpdate(ids[rand.Intn(DURABLE_BENCH_SIZE)], docs[rand.Intn(DURABLE_BENCH_SIZE)]); err != nil {
			panic("update error")
		}
	})
	average("delete", DURABLE_BENCH_SIZE, func() {}, func() {
		if err := col.DurableDelete(ids[rand.Intn(DURABLE_BENCH_SIZE)]); err != nil {
			panic("delete error")
		}
	})
	col.Close()
}
