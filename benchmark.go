// Benchmark of tiedot individual features and usages.
package main

import (
	"encoding/json"
	"fmt"
	"github.com/HouzuoGuo/tiedot/db"
	"math/rand"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

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
	// Print average time consumption and summary.
	end := float64(time.Now().UTC().UnixNano())
	fmt.Printf("%s %d: %d ns/iter, %d iter/sec\n", name, int(total), int((end-start)/iter), int(1000000000/((end-start)/iter)))
}

// Benchmark document insert, read, query, update and delete.
func benchmark(benchSize int) {
	docs := make([]interface{}, benchSize)
	ids := make([]uint64, benchSize)

	// Prepare serialized documents to be inserted
	for i := range docs {
		if err := json.Unmarshal([]byte(
			`{"a": {"b": {"c": `+strconv.Itoa(rand.Intn(benchSize))+`}},`+
				`"c": {"d": `+strconv.Itoa(rand.Intn(benchSize))+`},`+
				`"more": "abcdefghijklmnopqrstuvwxyz"}`), &docs[i]); err != nil {
			panic("json error")
		}
	}

	// Prepare a collection with two indexes
	tmp := "/tmp/tiedot_bench"
	os.RemoveAll(tmp)
	defer os.RemoveAll(tmp)
	col, err := db.OpenCol(tmp)
	if err != nil {
		panic(err)
	}
	col.Index([]string{"a", "b", "c"})
	col.Index([]string{"c", "d"})

	// Benchmark document insert
	average("insert", benchSize, func() {}, func() {
		if _, err := col.Insert(docs[rand.Intn(benchSize)]); err != nil {
			panic("insert error")
		}
	})

	// Collect all document IDs and benchmark document read
	average("read", benchSize, func() {
		col.ForAll(func(id uint64, doc interface{}) bool {
			ids = append(ids, id)
			return true
		})
	}, func() {
		var doc interface{}
		err := col.Read(ids[rand.Intn(benchSize)], &doc)
		if err != nil {
			panic(err)
		}
		if doc == nil {
			panic("read error")
		}
	})

	// Benchmark lookup query (two attributes)
	//average("lookup", benchSize, func() {}, func() {
	//	var query interface{}
	//	if err := json.Unmarshal([]byte(`{"c": [{"eq": `+strconv.Itoa(rand.Intn(benchSize))+`, "in": ["a", "b", "c"], "limit": 1}, `+
	//		`{"eq": `+strconv.Itoa(rand.Intn(benchSize))+`, "in": ["c", "d"], "limit": 1}]}`), &query); err != nil {
	//		panic("json error")
	//	}
	//	result := make(map[uint64]struct{})
	//	if err := db.EvalQueryV2(query, col, &result); err != nil {
	//		panic("query error")
	//	}
	//})

	// Benchmark document update
	average("update", benchSize, func() {}, func() {
		if _, err := col.Update(ids[rand.Intn(benchSize)], docs[rand.Intn(benchSize)]); err != nil {
			panic("update error")
		}
	})

	// Benchmark document delete
	average("delete", benchSize, func() {}, func() {
		col.Delete(ids[rand.Intn(benchSize)])
	})
	col.Close()
}

// Run document opearations (insert, read, query, update and delete) all at once.
func benchmark2(benchSize int) {
	docs := make([]uint64, 0, benchSize*2+1000)
	wp := new(sync.WaitGroup)
	numThreads := runtime.GOMAXPROCS(-1)
	wp.Add(5 * numThreads) // There are 5 goroutines: insert, read, query, update and delete

	// Prepare a collection with two indexes
	tmp := "/tmp/tiedot_bench"
	os.RemoveAll(tmp)
	col, err := db.OpenCol(tmp)
	if err != nil {
		panic(err)
	}
	col.Index([]string{"a", "b", "c"})
	col.Index([]string{"c", "d"})

	// Insert 1000 documents to make a start
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
	start := float64(time.Now().UTC().UnixNano())

	// Insert benchSize * 2 documents
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
					docs = append(docs, newID)
				} else {
					panic(err)
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
			var doc interface{}
			for j := 0; j < benchSize/numThreads*2; j++ {
				col.Read(docs[uint64(rand.Intn(len(docs)))], &doc)
			}
			fmt.Printf("Read thread %d completed\n", i)
		}(i)
	}

	// Query benchSize times (lookup on two attributes)
	//for i := 0; i < numThreads; i++ {
	//	go func(i int) {
	//		fmt.Printf("Query thread %d starting\n", i)
	//		defer wp.Done()
	//		var query interface{}
	//		var err error
	//		for j := 0; j < benchSize/numThreads; j++ {
	//			if err = json.Unmarshal([]byte(`{"c": [{"eq": `+strconv.Itoa(rand.Intn(benchSize))+`, "in": ["a", "b", "c"], "limit": 1}, `+
	//				`{"eq": `+strconv.Itoa(rand.Intn(benchSize))+`, "in": ["c", "d"], "limit": 1}]}`), &query); err != nil {
	//				panic("json error")
	//			}
	//			result := make(map[uint64]struct{})
	//			if err = db.EvalQueryV2(query, col, &result); err != nil {
	//				panic("query error")
	//			}
	//		}
	//		fmt.Printf("Query thread %d completed\n", i)
	//	}(i)
	//}

	// Update benchSize documents
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

	// Delete benchSize documents
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

	// Wait for all goroutines to finish, then print summary
	wp.Wait()
	end := float64(time.Now().UTC().UnixNano())
	fmt.Printf("Total operations %d: %d ns/iter, %d iter/sec\n", benchSize*7, int((end-start)/float64(benchSize)/7), int(1000000000/((end-start)/float64(benchSize)/7)))
}

// Benchmark document opeartions (insert, read, query, update and delete), address documents by unique ID (UID)
func benchmark3(benchSize int) {
	uidsMutex := new(sync.Mutex)
	uids := make([]string, 0, benchSize)

	// Prepare serialized documents to be inserted
	docs := make([]interface{}, benchSize)
	for i := range docs {
		if err := json.Unmarshal([]byte(
			`{"a": {"b": {"c": `+strconv.Itoa(rand.Intn(benchSize))+`}},`+
				`"c": {"d": `+strconv.Itoa(rand.Intn(benchSize))+`},`+
				`"more": "abcdefghijklmnopqrstuvwxyz"}`), &docs[i]); err != nil {
			panic("json error")
		}
	}

	// Prepare a collection with two indexes
	tmp := "/tmp/tiedot_bench"
	os.RemoveAll(tmp)
	defer os.RemoveAll(tmp)
	col, err := db.OpenCol(tmp)
	if err != nil {
		panic(err)
	}
	col.Index([]string{"a", "b", "c"})
	col.Index([]string{"c", "d"})

	// Benchmark insert document with UID
	average("insert", benchSize, func() {}, func() {
		if _, uid, err := col.InsertWithUID(docs[rand.Intn(benchSize)]); err == nil {
			uidsMutex.Lock()
			uids = append(uids, uid)
			uidsMutex.Unlock()
		} else {
			panic("insert error")
		}
	})

	// Benchmark read document by UID
	average("read", benchSize, func() {
	}, func() {
		var doc interface{}
		col.ReadByUID(uids[rand.Intn(benchSize)], &doc)
	})

	// Benchmark update document by UID
	average("update", benchSize, func() {}, func() {
		col.UpdateByUID(uids[rand.Intn(benchSize)], docs[rand.Intn(benchSize)])
	})

	// Benchmark delete document by UID
	average("delete", benchSize, func() {}, func() {
		col.DeleteByUID(uids[rand.Intn(benchSize)])
	})
	col.Close()
}
