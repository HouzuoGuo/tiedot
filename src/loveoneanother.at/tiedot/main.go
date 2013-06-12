package main

import (
	"encoding/json"
	"fmt"
	"loveoneanother.at/tiedot/db"
	"os"
	"time"
)

const (
	COL_BENCH_SIZE    = 5000000 // Number of documents made available for collection benchmark
	COL_BENCH_THREADS = 8       // Number of threads for collection benchmark
)

func main() {
	os.RemoveAll("/tmp/col")
	col, err := db.OpenCol("/tmp/col")
	if err != nil {
		fmt.Println(err)
		return
	}
	var jsonDoc interface{}
	json.Unmarshal([]byte(`{"a": 1}`), jsonDoc)
	completed := make(chan bool, COL_BENCH_THREADS)
	start := time.Now().UnixNano()
	for t := 0; t < COL_BENCH_THREADS; t++ {
		go func() {
			for d := 0; d < COL_BENCH_SIZE/COL_BENCH_THREADS; d++ {
				col.Insert(jsonDoc)
			}
			completed <- true
		}()
	}
	for c := 0; c < COL_BENCH_THREADS; c++ {
		<-completed
	}
	end := time.Now().UnixNano()
	fmt.Println(float64(COL_BENCH_SIZE) / (float64(end-start) / float64(1000000000.0)))
}
