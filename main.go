package main

import (
	"flag"
	"fmt"
	"github.com/HouzuoGuo/tiedot/network"
	"github.com/HouzuoGuo/tiedot/tdlog"
	"math/rand"
	"runtime"
)

const (
	BENCH_COL_NAME = "bench"
)

func main() {
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
		tdlog.Println("Will set GOMAXPROCS to 1 for optimal IPC performance")
		runtime.GOMAXPROCS(1)
		// Initialize and start IPC server
		server, err := network.NewServer(myRank, totalRank, dbDir, tmpDir)
		if err != nil {
			panic(err)
		}
		server.Start()
	case "bench-setup":
		// Connect to server rank 0 to setup everything for benchmark
		client, err := network.NewClient(tmpDir, 0)
		if err != nil {
			panic(err)
		}
		if err = client.ColCreate(BENCH_COL_NAME, totalRank); err != nil {
			panic(err)
		}
		if err = client.IdxCreate(BENCH_COL_NAME, "a,b"); err != nil {
			panic(err)
		}
	case "bench-client":
		// Run a client for benchmarking the server rank, benchmark begins immediately
		tdlog.Println("Will set GOMAXPROCS to 1 for optimal IPC performance")
		runtime.GOMAXPROCS(1)
		client, err := network.NewClient(tmpDir, myRank)
		if err != nil {
			panic(err)
		}
		for i := 0; i < benchSize; i++ {
			fmt.Println(i)
			if _, err = client.ColInsert(BENCH_COL_NAME, map[string]interface{}{"a": map[string]interface{}{"b": rand.Intn(benchSize)}}); err != nil {
				panic(err)
			}
		}
	default:
		flag.PrintDefaults()
	}
}
