/* Run tiedot HTTP API server, benchmarks, or embedded usage example. */
package main

import (
	"flag"
	"github.com/HouzuoGuo/tiedot/db"
	"github.com/HouzuoGuo/tiedot/httpapi"
	"github.com/HouzuoGuo/tiedot/tdlog"
	"github.com/HouzuoGuo/tiedot/webcp"
	"log"
	"os"
	"os/signal"
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
	var port, maxprocs int
	var profile, debug bool
	flag.StringVar(&mode, "mode", "", "[httpd|bench|bench2|example]")
	flag.StringVar(&dir, "dir", "", "(HTTP API) database directory")
	flag.IntVar(&port, "port", 8080, "(HTTP API) port number")
	flag.StringVar(&webcp.WebCp, "webcp", "admin", "(HTTP API) web control panel route (without leading slash)")
	flag.IntVar(&maxprocs, "gomaxprocs", defaultMaxprocs, "GOMAXPROCS")
	flag.IntVar(&benchSize, "benchsize", 400000, "Benchmark sample size")
	flag.BoolVar(&profile, "profile", false, "Write profiler results to prof.out")
	flag.BoolVar(&debug, "debug", false, "Dump goroutine stack traces upon receiving interrupt signal")
	flag.BoolVar(&tdlog.VerboseLog, "verbose", false, "Turn verbose logging on/off")
	flag.BoolVar(&benchCleanup, "benchcleanup", true, "Whether to clean up (delete benchmark DB) after benchmark")
	flag.Parse()

	// User must specify a mode to run
	if mode == "" {
		flag.PrintDefaults()
		return
	}

	// Set appropriate GOMAXPROCS
	runtime.GOMAXPROCS(maxprocs)
	tdlog.Noticef("GOMAXPROCS is set to %d", maxprocs)
	if maxprocs < runtime.NumCPU() {
		tdlog.Noticef("GOMAXPROCS (%d) is less than number of CPUs (%d), this may reduce performance. You can change it via environment variable GOMAXPROCS or by passing CLI parameter -gomaxprocs", maxprocs, runtime.NumCPU())
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

	// Dump goroutine stacktraces upon receiving interrupt signal
	if debug {
		c := make(chan os.Signal, 1)
		signal.Notify(c, os.Interrupt)
		go func() {
			for _ = range c {
				pprof.Lookup("goroutine").WriteTo(os.Stderr, 1)
			}
		}()
	}

	switch mode {
	case "httpd": // Run HTTP API server
		if dir == "" {
			tdlog.Panicf("Please specify database directory, for example -dir=/tmp/db")
		}
		if port == 0 {
			tdlog.Panicf("Please specify port number, for example -port=8080")
		}
		db, err := db.OpenDB(dir)
		if err != nil {
			panic(err)
		}
		httpapi.Start(db, port)
	case "example": // Run embedded usage examples
		embeddedExample()
	case "bench": // Benchmark scenarios
		benchmark()
	case "bench2":
		benchmark2()
	default:
		flag.PrintDefaults()
		return
	}
}
