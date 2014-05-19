package main

import (
	"flag"
	"fmt"
	"github.com/HouzuoGuo/tiedot/datasvc"
	"github.com/HouzuoGuo/tiedot/dbsvc"
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

var dumpStackOnInterrupt = false

func main() {

	// Common flags
	var mode, workDir, dbDir string
	flag.StringVar(&mode, "mode", "", "[datasvc|dbsvc|bench-setup|bench-client]")
	flag.StringVar(&workDir, "workdir", "", "Location of IPC server working directory (not data directory)")
	flag.StringVar(&dbDir, "dbdir", "", "Location of database directory")
	flag.BoolVar(&tdlog.VerboseLog, "verbose", true, "Turn verbose output on/off")
	flag.BoolVar(&dumpStackOnInterrupt, "dump-stack-on-interrupt", false, "Dump stack traces of all goroutines upon receiving interrupt signal")

	if dumpStackOnInterrupt {
		// Print all goroutine stacktraces on interrupt
		c := make(chan os.Signal, 1)
		signal.Notify(c, os.Interrupt, os.Kill)
		go func() {
			for {
				<-c
				pprof.Lookup("goroutine").WriteTo(os.Stdout, 1)
			}
		}()
	}

	// IPC/benchmark flags
	var myRank, totalRank int
	flag.IntVar(&myRank, "myrank", 0, "(datasvc only) My server rank number")
	flag.IntVar(&totalRank, "totalrank", 0, "(datasvc & dbsvc only) Total number of server ranks")

	// Benchmark flags
	var benchSize int
	flag.IntVar(&benchSize, "benchsize", 10000, "number of iterations in individual benchmark measure")

	flag.Parse()
	switch mode {
	case "datasvc":
		// Run a data structure server ("data partition")
		tdlog.Println("Setting GOMAXPROCS to 1 for optimal IPC server performance")
		runtime.GOMAXPROCS(1)
		// Initialize and start IPC server
		server := datasvc.NewDataSvc(workDir, myRank)
		if err := server.Serve(); err != nil {
			panic(err)
		}
	case "dbsvc":
		// Run a database server
		if totalRank < 1 {
			tdlog.Panicf("totalrank shall be greater than 0")
		}
		db, err := dbsvc.NewDBSvc(totalRank, workDir, dbDir)
		if err != nil {
			panic(err)
		}
		if err := db.Sync(); err != nil {
			panic(err)
		}
	case "bench-setup":
		// Prepare a collection with two indexes
		if totalRank < 1 {
			tdlog.Panicf("totalrank shall be greater than 0")
		}
		db, err := dbsvc.NewDBSvc(totalRank, workDir, dbDir)
		if err != nil {
			panic(err)
		} else if err := db.ColCreate(BENCH_COL_NAME); err != nil {
			panic(err)
		} else if err := db.IdxCreate(BENCH_COL_NAME, []string{"a"}); err != nil {
			panic(err)
		} else if err := db.IdxCreate(BENCH_COL_NAME, []string{"b"}); err != nil {
			panic(err)
		} else if err := db.Sync(); err != nil {
			panic(err)
		}
		fmt.Println("Benchmark setup completed")
	case "bench-client":
		// Benchmark sequence - begins immediately
		db, err := dbsvc.NewDBSvc(totalRank, workDir, dbDir)
		if err != nil {
			panic(err)
		}
		fmt.Println("Benchmark client ready")
		start := float64(time.Now().UnixNano())
		for i := 0; i < benchSize; i++ {
			doc := map[string]interface{}{
				"a":    strconv.Itoa(rand.Intn(benchSize)),
				"b":    strconv.Itoa(rand.Intn(benchSize)),
				"more": "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed mi sem, ultrices mollis nisl quis, convallis volutpat ante. Class aptent taciti sociosqu ad litora torquent per conubia nostra, per inceptos himenaeos. Proin interdum egestas risus, imperdiet vulputate est. Cras semper risus sit amet dolor facilisis malesuada. Nunc velit augue, accumsan id facilisis ultricies, vehicula eget massa. Ut non dui eu magna egestas aliquam. Fusce in pellentesque risus. Aliquam ornare pharetra lacus in rhoncus. In eu commodo nibh. Praesent at lacinia quam. Curabitur laoreet pellentesque mollis. Maecenas mollis bibendum neque. Pellentesque semper justo ac purus auctor cursus. In egestas sodales metus sed dictum. Vivamus at elit nunc. Phasellus sit amet augue sed augue rhoncus congue. Aenean et molestie augue. Aliquam blandit lacus eu nunc rhoncus, vitae varius mauris placerat. Quisque velit urna, pretium quis dolor et, blandit sodales libero. Nulla sollicitudin est vel dolor feugiat viverra massa nunc."}
			if _, err := db.DocInsert(BENCH_COL_NAME, doc); err != nil {
				panic(err)
			}
		}
		end := float64(time.Now().UnixNano())
		fmt.Println((end - start) / 1000000000.0)
	default:
		flag.PrintDefaults()
	}
}
