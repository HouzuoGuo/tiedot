package srv

import (
	"github.com/HouzuoGuo/tiedot/dsserver/colpart"
	"github.com/HouzuoGuo/tiedot/dsserver/dstruct"
	"github.com/HouzuoGuo/tiedot/tdlog"
	"io/ioutil"
	"net"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const (
	NUMCHUNKS_FILENAME      = "numchunks"
	HASHTABLE_DIRNAME_MAGIC = "ht_"    // Hash table directory name prefix
	CHUNK_DIRNAME_MAGIC     = "chunk_" // Chunk directory name prefix
	INDEX_PATH_SEP          = ","      // Separator between index path segments
)

const (
	// Hash operations
	HNEW  = "HNEW"
	HREMV = "HREMV"
	HSYNC = "HSYNC"
	HGT   = "HGT"
	HST   = "HST"
	HDE   = "HDE"
	// Collection operations
	CNEW  = "CNEW"
	CREMV = "CREMV"
	CSYNC = "CSYNC"
	CIN   = "CIN"
	CUP   = "CUP"
	CDE   = "CDE"
	// Other (Ops without params must have suffix new-line)
	SYNALL = "SYCALL\n"
	FIN    = "FIN\n"
	PING   = "PING\n"
	PONG   = "PONG\n"
)

// Tasks are queued on the server and executed one by one.
type Task struct {
	Ret chan interface{}   // Task result
	Fun func() interface{} // Task routine
}

// Server state and structures.
type Server struct {
	WorkingDir, DBDir string                                   // Working directory and DB directory
	Rank, TotalRank   int                                      // Rank of current process; total number of processes
	ColNumParts       map[string]int                           // Collection name -> number of partitions
	ColParts          map[string]*colpart.Partition            // Collection name -> partition
	Htables           map[string]map[string]*dstruct.HashTable // Collection name -> index name -> hash table
	Listener          net.Listener                             // This server socket
	InterRank         []net.Conn                               // Inter-rank communication connection
	MainLoop          chan *Task                               // Task loop
	Barrier           bool                                     // Placed when structural change is ongoing
}

// Start a new server.
func NewServer(rank, totalRank int, dbDir, workingDir string) (srv *Server, err error) {
	if rank >= totalRank {
		panic("rank >= totalRank - should never happen")
	}
	// Create both database and working directories
	if err = os.MkdirAll(dbDir, 0700); err != nil {
		return
	}
	if err = os.MkdirAll(workingDir, 0700); err != nil {
		return
	}
	srv = &Server{Rank: rank, TotalRank: totalRank,
		InterRank:   make([]net.Conn, totalRank),
		ColNumParts: make(map[string]int),
		ColParts:    make(map[string]*colpart.Partition),
		Htables:     make(map[string]map[string]*dstruct.HashTable),
		MainLoop:    make(chan *Task, 100)}
	// Create server socket
	serverSockFile := path.Join(workingDir, strconv.Itoa(rank))
	os.Remove(serverSockFile)
	srv.Listener, err = net.Listen("unix", serverSockFile)
	if err != nil {
		return
	}
	tdlog.Printf("Rank %d is listening on %s", rank, serverSockFile)
	// Contact other ranks (after 2 seconds delay)
	time.Sleep(2 * time.Second)
	for i := 0; i < totalRank; i++ {
		rankSockFile := path.Join(workingDir, strconv.Itoa(i))
		srv.InterRank[i], err = net.Dial("unix", rankSockFile)
		if err != nil {
			return
		}
		tdlog.Printf("Rank %d is now in contact with rank %d on %s", rank, i, rankSockFile)
	}
	// Open my partition of the database
	if err = srv.ReopenDB(); err != nil {
		return
	}
	// Start task worker
	go func() {
		for {
			task := <-srv.MainLoop
			task.Ret <- task.Fun()
		}
	}()
	return
}

// (Re)open my partition of the database.
func (server *Server) ReopenDB() (err error) {
	// Read the DB directory
	files, err := ioutil.ReadDir(server.DBDir)
	if err != nil {
		return
	}
	for _, f := range files {
		// Sub-directories are collections
		if f.IsDir() {
			// Read the "numchunks" file - its should contain a positive integer in the content
			var numchunksFH *os.File
			colName := f.Name()
			numchunksFH, err = os.OpenFile(path.Join(server.DBDir, colName, NUMCHUNKS_FILENAME), os.O_CREATE|os.O_RDWR, 0600)
			defer numchunksFH.Close()
			if err != nil {
				return
			}
			numchunksContent, err := ioutil.ReadAll(numchunksFH)
			if err != nil {
				panic(err)
			}
			numchunks, err := strconv.Atoi(string(numchunksContent))
			if err != nil || numchunks < 1 {
				tdlog.Panicf("Cannot figure out number of chunks for collection %s, manually repair it maybe? %v", server.DBDir, err)
			}
			server.ColNumParts[colName] = numchunks
			// If my rank is within the numeric range of collection partitions, go ahead and open my part
			if server.Rank < numchunks {
				tdlog.Printf("My rank is %d and I am going to open my partition in %s", server.Rank, f.Name())
				// Open data partition
				colDir := path.Join(server.DBDir, colName)
				part, err := colpart.OpenPart(path.Join(colDir, CHUNK_DIRNAME_MAGIC+strconv.Itoa(server.Rank)))
				if err != nil {
					return err
				}
				// Put the partition into server structure
				server.ColParts[colName] = part
				// Look for indexes in the collection
				walker := func(_ string, info os.FileInfo, err2 error) error {
					if err2 != nil {
						tdlog.Error(err)
						return nil
					}
					if info.IsDir() {
						switch {
						case strings.HasPrefix(info.Name(), HASHTABLE_DIRNAME_MAGIC):
							tdlog.Printf("My rank is %d and I am going to open my partition in hashtable %s", server.Rank, info.Name())
							// Figure out indexed path - including the partition number
							indexPathStr := info.Name()[len(HASHTABLE_DIRNAME_MAGIC):]
							indexPath := strings.Split(indexPathStr, INDEX_PATH_SEP)
							// Open a hash table index and put it into collection structure
							ht, err := dstruct.OpenHash(path.Join(colDir, info.Name(), strconv.Itoa(server.Rank)), indexPath)
							if err != nil {
								return err
							}
							server.Htables[colName][indexPathStr] = ht
						}
					}
					return nil
				}
				err = filepath.Walk(colDir, walker)
			}
		}
	}
	return nil
}

// Submit a task to the server and wait till its completion.
func (server *Server) Submit(task *Task) interface{} {
	server.MainLoop <- task
	return <-task.Ret
}
