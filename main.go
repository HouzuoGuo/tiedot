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
	"runtime/pprof"
)

func main() {
	// Parse CLI parameters
	var mode, dir string
	var port int
	var profile, debug bool
	flag.StringVar(&mode, "mode", "", "[httpd|bench|example]")
	flag.StringVar(&dir, "dir", "", "(HTTP API) database directory")
	flag.IntVar(&port, "port", 8080, "(HTTP API) port number")
	flag.StringVar(&webcp.WebCp, "webcp", "admin", "(HTTP API) web control panel route (without leading slash)")
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
	case "bench":
		benchmark()
	default:
		flag.PrintDefaults()
		return
	}
}
