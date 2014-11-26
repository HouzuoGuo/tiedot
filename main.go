/* Run tiedot HTTP API server, benchmarks, or embedded usage example. */
package main

import (
	"flag"
	"github.com/HouzuoGuo/tiedot/httpapi"
	"github.com/HouzuoGuo/tiedot/tdlog"
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

	// General params
	var mode string
	var maxprocs int
	flag.StringVar(&mode, "mode", "", "[httpd|bench|bench2|example]")
	flag.IntVar(&maxprocs, "gomaxprocs", defaultMaxprocs, "GOMAXPROCS")
	// Debug params
	var profile, debug bool
	flag.BoolVar(&tdlog.VerboseLog, "verbose", false, "Turn verbose logging on/off")
	flag.BoolVar(&profile, "profile", false, "Write profiler results to prof.out")
	flag.BoolVar(&debug, "debug", false, "Dump goroutine stack traces upon receiving interrupt signal")
	// HTTP mode params
	var dir string
	var port int
	var webcpRoute, tlsCrt, tlsKey string
	flag.StringVar(&dir, "dir", "", "(HTTP API) database directory")
	flag.StringVar(&webcpRoute, "webcp", "admin", "(HTTP API) web control panel route (without leading slash), 'no' to disable.")
	flag.IntVar(&port, "port", 8080, "(HTTP API) port number")
	flag.StringVar(&tlsCrt, "tlscrt", "", "(HTTP API) TLS certificate (TLS is optional, empty to disable).")
	flag.StringVar(&tlsKey, "tlskey", "", "(HTTP API) TLS certificate key (TLS is optional, empty to disable).")

	// HTTP + JWT params
	var jwtPubKey, jwtPrivateKey string
	flag.StringVar(&jwtPubKey, "jwtpubkey", "", "(HTTP with JWT) Public key for signing tokens")
	flag.StringVar(&jwtPrivateKey, "jwtprivatekey", "", "(HTTP with JWT) Private key for decoding tokens")

	// Benchmark mode params
	flag.IntVar(&benchSize, "benchsize", 400000, "Benchmark sample size")
	flag.BoolVar(&benchCleanup, "benchcleanup", true, "Whether to clean up (delete benchmark DB) after benchmark")
	flag.Parse()

	// User must specify a mode to run
	if mode == "" {
		flag.PrintDefaults()
		os.Exit(1)
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
			tdlog.Noticef("Cannot create profiler result file %s", resultFile)
			os.Exit(1)
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
	case "httpd":
		// Run HTTP API server
		if dir == "" {
			tdlog.Notice("Please specify database directory, for example -dir=/tmp/db")
			os.Exit(1)
		}
		if port == 0 {
			tdlog.Notice("Please specify port number, for example -port=8080")
			os.Exit(1)
		}
		if tlsCrt != "" && tlsKey == "" {
			tdlog.Notice("To enable HTTPS, please specify both RSA certificate and key file.")
			os.Exit(1)
		}
		if jwtPrivateKey != "" && jwtPubKey == "" {
			tdlog.Notice("To enable JWT, please specify RSA private and public key.")
			os.Exit(1)
		}
		httpapi.Start(dir, port, tlsCrt, tlsKey, webcpRoute, jwtPubKey, jwtPrivateKey)
	case "example":
		// Run embedded usage examples
		embeddedExample()
	case "bench":
		// Benchmark scenarios
		benchmark()
	case "bench2":
		benchmark2()
	default:
		flag.PrintDefaults()
		return
	}
}
