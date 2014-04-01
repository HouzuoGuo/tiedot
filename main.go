package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/HouzuoGuo/tiedot/network"
	"github.com/HouzuoGuo/tiedot/tdlog"
	"math/rand"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
	"strconv"
	"time"
)

const (
	BENCH_COL_NAME = "bench"
)

func main() {
	// Print all goroutine stacktraces on interrupt
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, os.Kill)
	go func() {
		for {
			<-c
			pprof.Lookup("goroutine").WriteTo(os.Stdout, 1)
		}
	}()

	// Common flags
	var mode, tmpDir, dbDir string
	flag.StringVar(&mode, "mode", "", "[ipc|bench-setup|bench-client|example]")
	flag.StringVar(&tmpDir, "tmpdir", "/tmp/tiedot_test_tmp", "Location of IPC server temporary files directory")
	flag.StringVar(&dbDir, "dbdir", "/tmp/tiedot_test_db", "Location of database directory")
	flag.BoolVar(&tdlog.VerboseLog, "verbose", true, "Turn verbose output on/off")

	// IPC/benchmark flags
	var myRank, totalRank int
	flag.IntVar(&myRank, "myrank", 0, "(IPC/benchmark Only) My (client|server) rank number")
	flag.IntVar(&totalRank, "totalrank", 0, "(IPC/benchmark Only) Total number of server ranks")

	// Benchmark flags
	var benchSize int
	flag.IntVar(&benchSize, "benchsize", 100000, "number of iterations in individual benchmark measure")

	flag.Parse()
	switch mode {
	case "ipc":
		// Run IPC server (only my rank)
		maxprocs := 1 + (totalRank-1)/2
		tdlog.Printf("Setting GOMAXPROCS to %d for optimal IPC server performance", maxprocs)
		runtime.GOMAXPROCS(maxprocs)
		// Initialize and start IPC server
		server, err := network.NewServer(myRank, totalRank, dbDir, tmpDir)
		if err != nil {
			panic(err)
		}
		server.Start()
	case "bench-setup":
		// Connect to server rank 0 to setup everything for benchmark
		client, err := network.NewClient(totalRank, tmpDir)
		if err != nil {
			panic(err)
		}
		if err = client.ColCreate(BENCH_COL_NAME); err != nil {
			panic(err)
		}
		if err = client.IdxCreate(BENCH_COL_NAME, "a"); err != nil {
			panic(err)
		}
		if err = client.IdxCreate(BENCH_COL_NAME, "b"); err != nil {
			panic(err)
		}
	case "bench-client":
		// Run a client for benchmarking the server rank, benchmark begins immediately
		client, err := network.NewClient(totalRank, tmpDir)
		if err != nil {
			panic(err)
		}
		start := float64(time.Now().UnixNano())
		for i := 0; i < benchSize; i++ {
			var doc map[string]interface{}
			if err := json.Unmarshal([]byte(
				`{"a": `+strconv.Itoa(rand.Intn(benchSize))+`, "b": `+strconv.Itoa(rand.Intn(benchSize))+`,
			"more": "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed mi sem, ultrices mollis nisl quis, convallis volutpat ante. Class aptent taciti sociosqu ad litora torquent per conubia nostra, per inceptos himenaeos. Proin interdum egestas risus, imperdiet vulputate est. Cras semper risus sit amet dolor facilisis malesuada. Nunc velit augue, accumsan id facilisis ultricies, vehicula eget massa. Ut non dui eu magna egestas aliquam. Fusce in pellentesque risus. Aliquam ornare pharetra lacus in rhoncus. In eu commodo nibh. Praesent at lacinia quam. Curabitur laoreet pellentesque mollis. Maecenas mollis bibendum neque. Pellentesque semper justo ac purus auctor cursus. In egestas sodales metus sed dictum. Vivamus at elit nunc. Phasellus sit amet augue sed augue rhoncus congue. Aenean et molestie augue. Aliquam blandit lacus eu nunc rhoncus, vitae varius mauris placerat. Quisque velit urna, pretium quis dolor et, blandit sodales libero. Nulla sollicitudin est vel dolor feugiat viverra massa nunc."}`), &doc); err != nil {
				panic("json error")
			}
			if _, err := client.ColInsert(BENCH_COL_NAME, doc); err != nil {
				panic(err)
			}
		}
		end := float64(time.Now().UnixNano())
		fmt.Println((end - start) / 1000000000.0)
	default:
		flag.PrintDefaults()
	}
}
