package main

import (
	"encoding/json"
	"fmt"
	"loveoneanother.at/tiedot/db"
	"math/rand"
	"os"
	"runtime"
	"strconv"
	"time"
)

func average(iterations int, do func()) {
	iter := float64(iterations)
	start := float64(time.Now().UTC().UnixNano())
	do()
	end := float64(time.Now().UTC().UnixNano())
	fmt.Printf("%d: %d ns/iter, %d iter/sec\n", int(iterations), int((end-start)/iter), int(1000000000/((end-start)/iter)))
}

func benchmark() {
	// config
	BENCH_SIZE := 1000000
	THREADS := 4
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
	// benchmark insert
	average(BENCH_SIZE, func() {
		completed := make(chan bool, BENCH_SIZE/THREADS)
		for i := 0; i < BENCH_SIZE; i += BENCH_SIZE / THREADS {
			go func() {
				for j := 0; j < BENCH_SIZE/THREADS; j++ {
					var jsonDoc interface{}
					if err := json.Unmarshal([]byte(
						`{"a": {"b": {"c": `+strconv.Itoa(rand.Int())+`}},`+
							`"c": {"d": `+strconv.Itoa(rand.Int())+`},`+
							`"more": "abcdefghijklmnopqrstuvwxyz"}`), &jsonDoc); err != nil {
						return
						panic("json error")
					}
					if _, err := col.Insert(jsonDoc); err != nil {
						return
						panic("insert error")
					}
				}
				completed <- true
			}()
		}
		for i := 0; i < BENCH_SIZE; i += BENCH_SIZE / THREADS {
			<-completed
		}
	})
	// benchmark update
	ids := make([]uint64, 0)
	col.ForAll(func(id uint64, doc interface{}) bool {
		ids = append(ids, id)
		return true
	})
	average(BENCH_SIZE, func() {
		completed := make(chan bool, BENCH_SIZE/THREADS)
		for i := 0; i < BENCH_SIZE; i += BENCH_SIZE / THREADS {
			go func() {
				for j := 0; j < BENCH_SIZE/THREADS; j++ {
					var jsonDoc interface{}
					if err := json.Unmarshal([]byte(
						`{"a": {"b": {"c": `+strconv.Itoa(rand.Int())+`}},`+
							`"c": {"d": `+strconv.Itoa(rand.Int())+`},`+
							`"more": "abcdefghijklmnopqrstuvwxyz"}`), &jsonDoc); err != nil {
						return
						panic("json error")
					}
					if _, err := col.Update(ids[rand.Intn(BENCH_SIZE)], jsonDoc); err != nil {
						return
						panic("insert error")
					}
				}
				completed <- true
			}()
		}
		for i := 0; i < BENCH_SIZE; i += BENCH_SIZE / THREADS {
			<-completed
		}
	})
	col.Close()
}

func main() {
	benchmark()
}
