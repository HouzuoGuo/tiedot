package main

import (
	"flag"
	"fmt"
	"github.com/HouzuoGuo/tiedot/network"
	"github.com/HouzuoGuo/tiedot/tdlog"
	"math/rand"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
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
		if err = client.IdxCreate(BENCH_COL_NAME, "a,b"); err != nil {
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
			if _, err = client.ColInsert(BENCH_COL_NAME, map[string]interface{}{"a": map[string]interface{}{"b": rand.Intn(benchSize)}}); err != nil {
				panic(err)
			}
		}
		end := float64(time.Now().UnixNano())
		fmt.Println((end - start) / 1000000000.0)
	default:
		flag.PrintDefaults()
	}
}
