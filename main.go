package main

import (
	"flag"
	"github.com/HouzuoGuo/tiedot/server"
	"github.com/HouzuoGuo/tiedot/tdlog"
	"runtime"
)

func main() {
	// Common flags
	var mode, tmpDir, dbDir string
	flag.StringVar(&mode, "mode", "", "[ipc|http|bench1|bench2|example]")
	flag.StringVar(&tmpDir, "tmpdir", "/tmp/tiedot_tmp", "Location of temporary files directory")
	flag.StringVar(&dbDir, "dbdir", "/tmp/tiedot_db", "Location of database directory")
	flag.BoolVar(&tdlog.VerboseLog, "verbose", true, "Turn verbose output on/off")

	// IPC flags
	var myRank, totalRank int
	flag.IntVar(&myRank, "myrank", 0, "(IPC Only) My rank number")
	flag.IntVar(&totalRank, "totalrank", 0, "(IPC Only) Total rank number")

	flag.Parse()
	switch mode {
	case "ipc": // this mode is reserved for internal use
		tdlog.Println("Will set GOMAXPROCS to 1 for optimal IPC performance")
		runtime.GOMAXPROCS(1)
		// Initialize and start IPC server
		server, err := server.NewServer(myRank, totalRank, dbDir, tmpDir)
		if err != nil {
			panic(err)
		}
		server.Start()
	default:
		flag.PrintDefaults()
	}
}
