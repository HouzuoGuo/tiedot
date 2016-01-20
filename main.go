// Run tiedot HTTP API server, benchmarks, or embedded usage example.
package main

import (
	"flag"
	"io/ioutil"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
	"strconv"
	"strings"

	"github.com/HouzuoGuo/tiedot/httpapi"
	"github.com/HouzuoGuo/tiedot/tdlog"
)

// Read Linux system VM parameters and print performance configuration advice when necessary.
func linuxPerfAdvice() {
	readFileIntContent := func(filePath string) (contentInt int, err error) {
		content, err := ioutil.ReadFile(filePath)
		if err != nil {
			return
		}
		contentInt, err = strconv.Atoi(strings.TrimSpace(string(content)))
		return
	}
	swappiness, err := readFileIntContent("/proc/sys/vm/swappiness")
	if err != nil {
		tdlog.Notice("Non-fatal - unable to offer performance advice based on vm.swappiness.")
	} else if swappiness > 30 {
		tdlog.Noticef("System vm.swappiness is very high (%d), for optimium performance please lower it below 30.", swappiness)
	}
	dirtyRatio, err := readFileIntContent("/proc/sys/vm/dirty_ratio")
	if err != nil {
		tdlog.Notice("Non-fatal - unable to offer performance advice based on vm.dirty_ratio.")
	} else if dirtyRatio < 50 {
		tdlog.Noticef("System vm.dirty_ratio is very low (%d), for optimium performance please raise it above 50.", dirtyRatio)
	}
	dirtyBGRatio, err := readFileIntContent("/proc/sys/vm/dirty_background_ratio")
	if err != nil {
		tdlog.Notice("Non-fatal - unable to offer performance advice based on vm.dirty_background_ratio.")
	} else if dirtyBGRatio > 20 {
		tdlog.Noticef("System vm.dirty_background_ratio is very high (%d), for optimium performance please lower it below 20.", dirtyBGRatio)
	}
}

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
	flag.StringVar(&mode, "mode", "", "Mandatory - specify the execution mode [httpd|bench|bench2|example]")
	flag.IntVar(&maxprocs, "gomaxprocs", defaultMaxprocs, "GOMAXPROCS")
	// Debug params
	var profile, debug bool
	flag.BoolVar(&tdlog.VerboseLog, "verbose", false, "Turn verbose logging on/off")
	flag.BoolVar(&profile, "profile", false, "Write profiler results to prof.out")
	flag.BoolVar(&debug, "debug", false, "Dump goroutine stack traces upon receiving interrupt signal")
	// HTTP mode params
	var dir string
	var bind string
	var port int
	var authToken string
	var tlsCrt, tlsKey string
	flag.StringVar(&dir, "dir", "", "(HTTP server) database directory")
	flag.StringVar(&bind, "bind", "", "(HTTP server) bind to IP address (all network interfaces by default)")
	flag.IntVar(&port, "port", 8080, "(HTTP server) port number")
	flag.StringVar(&tlsCrt, "tlscrt", "", "(HTTP server) TLS certificate (empty to disable TLS).")
	flag.StringVar(&tlsKey, "tlskey", "", "(HTTP server) TLS certificate key (empty to disable TLS).")
	flag.StringVar(&authToken, "authtoken", "", "(HTTP server) Only authorize requests carrying this token in 'Authorization: token TOKEN' header. (empty to disable)")

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

	// Set appropriate GOMAXPROCS
	runtime.GOMAXPROCS(maxprocs)
	tdlog.Noticef("GOMAXPROCS is set to %d", maxprocs)

	// Performance advices
	if maxprocs < runtime.NumCPU() {
		tdlog.Noticef("GOMAXPROCS (%d) is less than number of CPUs (%d), this may reduce performance. You can change it via environment variable GOMAXPROCS or by passing CLI parameter -gomaxprocs", maxprocs, runtime.NumCPU())
	}
	linuxPerfAdvice()

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
			for range c {
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
		if tlsCrt != "" && tlsKey == "" || tlsKey != "" && tlsCrt == "" {
			tdlog.Notice("To enable HTTPS, please specify both RSA certificate and key file.")
			os.Exit(1)
		}
		if jwtPrivateKey != "" && jwtPubKey == "" || jwtPubKey != "" && jwtPrivateKey == "" {
			tdlog.Notice("To enable JWT, please specify RSA private and public key.")
			os.Exit(1)
		}
		httpapi.Start(dir, port, tlsCrt, tlsKey, jwtPubKey, jwtPrivateKey, bind, authToken)
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
