/* Run tiedot HTTP API server, benchmarks, or embedded usage example. */
package main

import (
	"flag"
	"github.com/HouzuoGuo/tiedot/httpapi"
	"github.com/HouzuoGuo/tiedot/tdlog"
	"os"
	"os/signal"
	"runtime/pprof"
)

func main() {
	// Parse CLI parameters
	// General params
	var mode string
	var maxprocs int
	flag.StringVar(&mode, "mode", "", "[httpd|bench|example]")
	flag.IntVar(&maxprocs, "gomaxprocs", 1, "GOMAXPROCS")

	// Debug params
	var profile, debug bool
	flag.BoolVar(&tdlog.VerboseLog, "verbose", false, "Turn verbose logging on/off")
	flag.BoolVar(&profile, "profile", false, "Write profiler results to prof.out")
	flag.BoolVar(&debug, "debug", false, "Dump goroutine stack traces upon receiving interrupt signal")

	// HTTP mode params
	var httpDBDir string
	var httpPort int
	var httpTLSCrt, httpTLSKey string
	flag.StringVar(&httpDBDir, "httpdbdir", "", "(HTTP server) database directory")
	flag.IntVar(&httpPort, "httpport", 8080, "(HTTP server) port number")
	flag.StringVar(&httpTLSCrt, "httptlscrt", "", "(HTTP server) TLS certificate (empty to disable TLS).")
	flag.StringVar(&httpTLSKey, "httptlskey", "", "(HTTP server) TLS certificate key (empty to disable TLS).")

	// HTTP + JWT params
	var jwtPubKey, jwtPrivateKey string
	flag.StringVar(&jwtPubKey, "jwtpubkey", "", "(HTTP JWT server) Public key for signing tokens (empty to disable JWT)")
	flag.StringVar(&jwtPrivateKey, "jwtprivatekey", "", "(HTTP JWT server) Private key for decoding tokens (empty to disable JWT)")

	// Benchmark mode params
	flag.IntVar(&benchSize, "benchsize", 400000, "Benchmark sample size")
	flag.BoolVar(&benchCleanup, "benchcleanup", true, "Whether to clean up (delete benchmark DB) after benchmark")
	flag.Parse()

	// User must specify a mode to run
	if mode == "" {
		flag.PrintDefaults()
		os.Exit(1)
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
		if httpDBDir == "" {
			tdlog.Notice("Please specify database directory, for example -httpdbdir=/tmp/db")
			os.Exit(1)
		}
		if httpPort == 0 {
			tdlog.Notice("Please specify port number, for example -httpport=19993")
			os.Exit(1)
		}
		if httpTLSCrt != "" && httpTLSKey == "" {
			tdlog.Notice("To enable HTTPS, please specify both RSA certificate and key file.")
			os.Exit(1)
		}
		if jwtPrivateKey != "" && jwtPubKey == "" {
			tdlog.Notice("To enable JWT, please specify RSA private and public key.")
			os.Exit(1)
		}
		httpapi.Start(httpDBDir, httpPort, httpTLSCrt, httpTLSKey, jwtPubKey, jwtPrivateKey)
	case "example":
		// Run embedded usage examples
		embeddedExample()
	case "bench":
		benchmark()
	default:
		flag.PrintDefaults()
		return
	}
}
