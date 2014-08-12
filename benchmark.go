package main

import (
	"encoding/json"
	"fmt"
	"github.com/HouzuoGuo/tiedot/db"
	"math/rand"
	"os"
	"strconv"
	"time"
)

// Whether to clean up (delete benchmark DB) after benchmark
var benchCleanup = true

// Size of benchmark sample
var benchSize = 400000

// Run the benchmark function a number of times, and print out performance data.
func average(name string, fun func()) {
	iter := float64(benchSize)
	start := float64(time.Now().UTC().UnixNano())
	// Run function across multiple goroutines
	for i := 0; i < benchSize; i++ {
		fun()
	}
	end := float64(time.Now().UTC().UnixNano())
	fmt.Printf("%s %d: %d ns/iter, %d iter/sec\n", name, int(benchSize), int((end-start)/iter), int(1000000000/((end-start)/iter)))
}

// Create a temporary database and collection for benchmark use.
func mkTmpDBAndCol(dbPath string, colName string) (*db.DB, *db.Col) {
	os.RemoveAll(dbPath)
	tmpDB, err := db.OpenDB(dbPath)
	if err != nil {
		panic(err)
	}
	if err = tmpDB.Create(colName); err != nil {
		panic(err)
	}
	return tmpDB, tmpDB.Use(colName)
}

func sampleDoc() (js map[string]interface{}) {
	doc := fmt.Sprintf(`
{
	"nested": {
		"nested": {
			"str": "%s",
			"int": %d,
			"float": %f
		}
	},
	"strs": ["%s", "%s"],
	"ints": [%d, %d],
	"floats": [%f, %f]
}
`, strconv.FormatFloat(rand.Float64(), 'f', 6, 64), rand.Intn(benchSize), rand.Float64(),
		strconv.FormatFloat(rand.Float64(), 'f', 6, 64), strconv.FormatFloat(rand.Float64(), 'f', 6, 64),
		rand.Intn(benchSize), rand.Intn(benchSize),
		rand.Float32(), rand.Float32())
	if err := json.Unmarshal([]byte(doc), &js); err != nil {
		panic(err)
	}
	return
}

func sampleQuery() (js interface{}) {
	rangeStart := rand.Intn(benchSize)
	q := fmt.Sprintf(`
[
	{ "c": [
		{"eq": %d, "in": ["nested", "nested", "int"]},
		{"eq": %d, "in": ["ints"]}
	] },
	{ "int-from": %d, "int-to": %d, "in": ["ints"]}
]
`, rand.Intn(benchSize), rand.Intn(benchSize), rangeStart, rangeStart+2)
	if err := json.Unmarshal([]byte(q), &js); err != nil {
		panic(err)
	}
	return
}

// Document CRUD benchmark (insert/read/query/update/delete), intended for catching performance regressions.
func benchmark() {
	ids := make([]uint64, 0, benchSize)
	// Prepare a collection with two indexes
	tmp := "/tmp/tiedot_bench"
	if benchCleanup {
		defer os.RemoveAll(tmp)
	}
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
			fmt.Println("Insert error", err)
		}
	})

	// Collect all document IDs and benchmark document read
	col.ForEachDoc(func(id uint64, _ []byte) bool {
		ids = append(ids, id)
		return true
	})
	average("read", func() {
		doc, err := col.Read(ids[rand.Intn(benchSize)])
		if doc == nil || err != nil {
			fmt.Println("Read error", doc, err)
		}
	})

	// Benchmark lookup query (two attributes)
	average("lookup", func() {
		result := make(map[uint64]struct{})
		if err := db.EvalQuery(sampleQuery(), col, &result); err != nil {
			fmt.Println("Query error", err)
		}
	})

	// Benchmark document update
	average("update", func() {
		if err := col.Update(ids[rand.Intn(benchSize)], sampleDoc()); err != nil {
			fmt.Println("Update error", err)
		}
	})

	// Benchmark document delete
	var delCount uint64
	average("delete", func() {
		if err := col.Delete(ids[rand.Intn(benchSize)]); err == nil {
			delCount++
		}
	})
	fmt.Printf("Deleted %d documents\n", delCount)
}
