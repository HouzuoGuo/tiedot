package main

import (
	"flag"
	"log"
	"loveoneanother.at/tiedot/db"
	"loveoneanother.at/tiedot/srv/v1"
	"runtime"
)

func main() {
	var mode, dir string
	var port, maxprocs int
	flag.StringVar(&mode, "mode", "", "[v1|bench]")
	flag.StringVar(&dir, "dir", "", "database directory")
	flag.IntVar(&port, "port", 0, "listening port number")
	flag.IntVar(&maxprocs, "gomaxprocs", runtime.NumCPU()*2, "GOMAXPROCS")
	flag.Parse()

	if mode == "" {
		log.Fatal("tiedot -mode=[v1|bench] -gomaxprocs=MAX_NUMBER_OF_GOPROCS")
	}

	runtime.GOMAXPROCS(maxprocs)
	log.Printf("GOMAXPROCS is set to %d", maxprocs)

	switch mode {
	case "v1":
		if dir == "" {
			log.Fatal("Please specify database directory, for example -dir=/tmp/db")
		}
		if port == 0 {
			log.Fatal("Plese specify port number, for example -port=8080")
		}
		db, err := db.OpenDB(dir)
		if err != nil {
			log.Fatal(err)
		}
		v1.Start(db, port)
	case "bench":
		benchmark()
	default:
		log.Fatal("tiedot -mode=[v1|bench] -gomaxprocs=MAX_NUMBER_OF_GOPROCS")
	}
}
