package main

import (
	"flag"
	"log"
	"loveoneanother.at/tiedot/db"
	"loveoneanother.at/tiedot/srv/v1"
	"loveoneanother.at/tiedot/srv/v2"
	"loveoneanother.at/tiedot/srv/v3"
	"os"
	"runtime"
	"runtime/pprof"
	"strconv"
)

func main() {
	var err error

	var defaultMaxprocs int
	if defaultMaxprocs, err = strconv.Atoi(os.Getenv("GOMAXPROCS")); err != nil {
		defaultMaxprocs = runtime.NumCPU() * 2
	}

	// parse CLI parameters
	var mode, dir string
	var port, maxprocs, benchSize int
	var profile bool
	flag.StringVar(&mode, "mode", "", "[v1|v2|v3|bench|bench2|bench3|durable-bench|example]")
	flag.StringVar(&dir, "dir", "", "database directory")
	flag.IntVar(&port, "port", 0, "listening port number")
	flag.IntVar(&maxprocs, "gomaxprocs", defaultMaxprocs, "GOMAXPROCS")
	flag.IntVar(&benchSize, "benchsize", 400000, "Benchmark sample size")
	flag.BoolVar(&profile, "profile", false, "write profiler results to prof.out")
	flag.Parse()

	if mode == "" {
		flag.PrintDefaults()
		return
	}

	// set appropriate gomaxprocs
	runtime.GOMAXPROCS(maxprocs)
	log.Printf("GOMAXPROCS is set to %d", maxprocs)
	if maxprocs < runtime.NumCPU() {
		log.Printf("GOMAXPROCS (%d) is less than number of CPUs (%d), this may affect performance. You can change it via environment variable GOMAXPROCS or by passing CLI parameter -gomaxprocs", maxprocs, runtime.NumCPU())
	}

	// enable profiler
	if profile {
		resultFile, err := os.Create("perf.out")
		if err != nil {
			log.Panicf("Cannot create profiler result file %s", resultFile)
		}
		pprof.StartCPUProfile(resultFile)
		defer pprof.StopCPUProfile()
	}

	switch mode {
	case "v1":
		fallthrough
	case "v2":
		fallthrough
	case "v3":
		if dir == "" {
			log.Fatal("Please specify database directory, for example -dir=/tmp/db")
		}
		if port == 0 {
			log.Fatal("Please specify port number, for example -port=8080")
		}
		db, err := db.OpenDB(dir)
		if err != nil {
			log.Fatal(err)
		}
		if mode == "v1" {
			v1.Start(db, port)
		} else if mode == "v2" {
			v2.Start(db, port)
		} else if mode == "v3" {
			v3.Start(db, port)
		}
	case "bench":
		benchmark(benchSize)
	case "bench2":
		benchmark2(benchSize)
	case "bench3":
		benchmark3(benchSize)
	case "durable-bench":
		durableBenchmark()
	case "example":
		embeddedExample()
	default:
		flag.PrintDefaults()
		return
	}
}
