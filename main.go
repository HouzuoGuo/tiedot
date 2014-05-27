package main

import (
	"flag"
	"github.com/HouzuoGuo/tiedot/db"
	"github.com/HouzuoGuo/tiedot/httpapi"
	"github.com/HouzuoGuo/tiedot/tdlog"
	"log"
	"os"
	"runtime"
	"runtime/pprof"
	"strconv"
)

func main() {
	var err error
	var defaultMaxprocs int
	if defaultMaxprocs, err = strconv.Atoi(os.Getenv("GOMAXPROCS")); err != nil {
		defaultMaxprocs = runtime.NumCPU()
	}

	// Parse CLI parameters
	var mode, dir string
	var port, maxprocs, benchSize int
	var profile bool
	flag.StringVar(&mode, "mode", "", "[httpd|bench|bench2|example]")
	flag.StringVar(&dir, "dir", "", "(HTTP API) database directory")
	flag.IntVar(&port, "port", 8080, "(HTTP API) port number")
	flag.IntVar(&maxprocs, "gomaxprocs", defaultMaxprocs, "GOMAXPROCS")
	flag.IntVar(&benchSize, "benchsize", 400000, "Benchmark sample size")
	flag.BoolVar(&profile, "profile", false, "Write profiler results to prof.out")
	flag.BoolVar(&tdlog.VerboseLog, "verbose", false, "Turn verbose logging on/off")
	flag.Parse()

	// User must specify a mode to run
	if mode == "" {
		flag.PrintDefaults()
		return
	}

	// Set appropriate GOMAXPROCS
	runtime.GOMAXPROCS(maxprocs)
	log.Printf("GOMAXPROCS is set to %d", maxprocs)
	if maxprocs < runtime.NumCPU() {
		tdlog.Printf("GOMAXPROCS (%d) is less than number of CPUs (%d), this may reduce performance. You can change it via environment variable GOMAXPROCS or by passing CLI parameter -gomaxprocs", maxprocs, runtime.NumCPU())
	}

	// Start profiler if enabled
	if profile {
		resultFile, err := os.Create("perf.out")
		if err != nil {
			log.Panicf("Cannot create profiler result file %s", resultFile)
		}
		pprof.StartCPUProfile(resultFile)
		defer pprof.StopCPUProfile()
	}

	switch mode {
	case "httpd": // Run HTTP API server
		if dir == "" {
			tdlog.Fatal("Please specify database directory, for example -dir=/tmp/db")
		}
		if port == 0 {
			tdlog.Fatal("Please specify port number, for example -port=8080")
		}
		db, err := db.OpenDB(dir)
		if err != nil {
			tdlog.Fatal(err)
		}
		httpapi.Start(db, port)
	case "example": // Run embedded usage examples
		embeddedExample()
	case "bench": // Benchmark scenarios
		benchmark(benchSize)
	case "bench2":
		benchmark2(benchSize)
	default:
		flag.PrintDefaults()
		return
	}
}
