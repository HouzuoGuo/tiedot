package sharding

import (
	"encoding/json"
	"fmt"
	"github.com/HouzuoGuo/tiedot/data"
	"github.com/HouzuoGuo/tiedot/tdlog"
	"math/rand"
	"os"
	"os/exec"
	"strconv"
	"time"
)

const BENCH_COL_NAME = "benchmark"

func RunBenchSupervisor(benchSize int) {
	dbdir := "/tmp/tiedot_bench" + strconv.Itoa(int(time.Now().UnixNano()))
	// Run the IPC servers just like usual
	go RunIPCServerSupervisor(dbdir)
	// Servers should be ready in two seconds
	time.Sleep(2 * time.Second)
	// Prepare benchmark collection and indexes
	client, err := NewClient(dbdir)
	if err == nil {
		err = client.Create(BENCH_COL_NAME)
	}
	if err == nil {
		err = client.Index(BENCH_COL_NAME, []string{"nested", "nested", "str"})
	}
	if err == nil {
		err = client.Index(BENCH_COL_NAME, []string{"nested", "nested", "int"})
	}
	if err == nil {
		err = client.Index(BENCH_COL_NAME, []string{"nested", "nested", "float"})
	}
	if err == nil {
		err = client.Index(BENCH_COL_NAME, []string{"strs"})
	}
	if err == nil {
		err = client.Index(BENCH_COL_NAME, []string{"ints"})
	}
	if err == nil {
		err = client.Index(BENCH_COL_NAME, []string{"floats"})
	}
	var dbfs *data.DBDirStruct
	if err == nil {
		dbfs, err = data.DBReadDir(dbdir)
	}
	if err != nil || len(dbfs.GetCollectionNamesSorted()) != 1 {
		tdlog.Panicf("Benchmark preparation has failed: %v", err)
	}
	// Run benchmark client processes
	procs := make([]*exec.Cmd, dbfs.NShards)
	for i := 0; i < dbfs.NShards; i++ {
		newproc := exec.Command(getMyExecutablePath(),
			"-mode=ipc-bench-process",
			"-ipcdbdir="+dbdir,
			"-benchsize="+strconv.Itoa(benchSize),
			"-gomaxprocs=1")
		newproc.Stdout = os.Stdout
		newproc.Stderr = os.Stderr
		procs[i] = newproc
	}
	for i := 0; i < dbfs.NShards; i++ {
		procs[i].Start()
	}
	for i := 0; i < dbfs.NShards; i++ {
		procs[i].Wait()
	}
}

// Run the benchmark function a number of times, and print out performance data.
func TimeAverage(name string, total int, fun func()) {
	iter := float64(total)
	start := float64(time.Now().UTC().UnixNano())
	// Run function across multiple goroutines
	for i := 0; i < total; i++ {
		fun()
	}
	end := float64(time.Now().UTC().UnixNano())
	fmt.Printf("%s %d: %d ns/iter, %d iter/sec\n", name, int(total), int((end-start)/iter), int(1000000000/((end-start)/iter)))
}

func sampleDoc(valueMax int) (js map[string]interface{}) {
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
`, strconv.FormatFloat(rand.Float64(), 'f', 6, 64), rand.Intn(valueMax), rand.Float64(),
		strconv.FormatFloat(rand.Float64(), 'f', 6, 64), strconv.FormatFloat(rand.Float64(), 'f', 6, 64),
		rand.Intn(valueMax), rand.Intn(valueMax),
		rand.Float32(), rand.Float32())
	if err := json.Unmarshal([]byte(doc), &js); err != nil {
		panic(err)
	}
	return
}

func sampleQuery(valueMax int) (js interface{}) {
	rangeStart := rand.Intn(valueMax)
	q := fmt.Sprintf(`
[
	{ "c": [
		{"eq": %d, "in": ["nested", "nested", "int"]},
		{"eq": %d, "in": ["ints"]}
	] },
	{ "int-from": %d, "int-to": %d, "in": ["ints"]}
]
`, rand.Intn(valueMax), rand.Intn(valueMax), rangeStart, rangeStart+2)
	if err := json.Unmarshal([]byte(q), &js); err != nil {
		panic(err)
	}
	return
}

func RunBenchProcess(dbdir string, totalBenchSize int) {
	// Start client
	client, err := NewClient(dbdir)
	if err != nil {
		panic(err)
	}

	benchSize := totalBenchSize / client.dbo.GetDBFS().NShards

	// Start benchmark
	docIDs := make([]uint64, benchSize)
	i := 0
	TimeAverage("insert", benchSize, func() {
		docID, err := client.Insert(BENCH_COL_NAME, sampleDoc(totalBenchSize))
		if err != nil {
			fmt.Println("Insert error", err)
		}
		docIDs[i] = docID
		i++
	})
	TimeAverage("read", benchSize, func() {
		doc, err := client.Read(BENCH_COL_NAME, docIDs[rand.Intn(benchSize)])
		if doc == nil || err != nil {
			fmt.Println("Read error", doc, err)
		}
	})
	TimeAverage("lookup", benchSize, func() {
		result := make(map[uint64]struct{})
		if err := client.EvalQuery(BENCH_COL_NAME, sampleQuery(totalBenchSize), &result); err != nil {
			fmt.Println("Query error", err)
		}
	})
	TimeAverage("update", benchSize, func() {
		if err := client.Update(BENCH_COL_NAME, docIDs[rand.Intn(benchSize)], sampleDoc(totalBenchSize)); err != nil {
			fmt.Println("Update error", err)
		}
	})
	var delCount uint64
	TimeAverage("delete", benchSize, func() {
		if err := client.Delete(BENCH_COL_NAME, docIDs[rand.Intn(benchSize)]); err == nil {
			delCount++
		}
	})
	fmt.Printf("Deleted %d documents\n", delCount)

}
