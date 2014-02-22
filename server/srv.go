/* Server structure and command loop. */
package server

import (
	"bufio"
	"fmt"
	"github.com/HouzuoGuo/tiedot/colpart"
	"github.com/HouzuoGuo/tiedot/dstruct"
	"github.com/HouzuoGuo/tiedot/tdlog"
	"math/rand"
	"net"
	"os"
	"path"
	"strconv"
	"strings"
	"time"
)

// Command operations
const (
	// Collection document manipulation with index updates
	COL_INSERT = "cin" // cin <col_name> <json_str>
	COL_UPDATE = "cup" // cup <col_name> <id> <json_str>
	COL_DELETE = "cde" // cde <col_name> <id>

	// Document CRUD (no index update)
	DOC_INSERT = "din" // din <col_name> <json_str>
	DOC_GET    = "dgt" // dgt <col_name> <id>
	DOC_UPDATE = "dup" // dup <col_name> <id> <json_str>
	DOC_DELETE = "dde" // dde <col_name> <id>

	// Index entry manipulation
	HT_SET    = "hst" // hst <col_name> <idx_name> <key> <val>
	HT_GET    = "hgt" // hgt <col_name> <idx_name> <key> <limit>
	HT_DELETE = "hde" // hde <col_name> <idx_name> <key> <val>

	// Collection management
	COL_CREATE = "col_create" // col_create <col_name> <num_parts>
	COL_ALL    = "col_all"    // col_all
	COL_RENAME = "col_rename" // col_rename <old_name> <new_name>
	COL_DROP   = "col_drop"   // col_drop <col_name>
	COL_SCRUB  = "col_scrub"  // col_scrub <col_name>
	COL_REPART = "col_repart" // col_repart <col_name> <new_num_parts>

	// Index management
	IDX_CREATE = "idx_create" // idx_create <col_name> <idx_path>
	IDX_ALL    = "idx_all"    // idx_all <col_name>
	IDX_DROP   = "idx_drop"   // idx_drop <col_name> <idx_path>

	// Other (Ops without params must have suffix new-line)
	RELOAD    = "reload"
	FLUSH_ALL = "flush"
	SHUTDOWN  = "shutdown"
	ACK       = "0" // Acknowledgement
)

// Tasks are queued on a server and executed one by one
type Task struct {
	Ret   chan interface{}              // Signal of function completion
	Input interface{}                   // Input to the task function
	Fun   func(interface{}) interface{} // Task (function) with a return value type
}

// Server state and structures.
type Server struct {
	WorkingDir, DBDir string                                   // Working directory and DB directory
	ServerSock        string                                   // Server socket file name
	Rank, TotalRank   int                                      // Rank of current process; total number of processes
	ColNumParts       map[string]int                           // Collection name -> number of partitions
	ColParts          map[string]*colpart.Partition            // Collection name -> partition
	Htables           map[string]map[string]*dstruct.HashTable // Collection name -> index name -> hash table
	Listener          net.Listener                             // This server socket
	InterRank         []*net.Conn                              // Inter-rank communication connection
	InterRankFeedback []chan interface{}                       // Inter-rank task feedback
	MainLoop          chan *Task                               // Task loop
}

// Start a new server.
func NewServer(rank, totalRank int, dbDir, workingDir string) (srv *Server, err error) {
	// It is important to seed random number generator!
	rand.Seed(time.Now().UnixNano())
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
		ServerSock: path.Join(workingDir, strconv.Itoa(rank)),
		WorkingDir: workingDir, DBDir: dbDir,
		InterRank:         make([]*net.Conn, totalRank),
		InterRankFeedback: make([]chan interface{}, totalRank),
		ColNumParts:       make(map[string]int),
		ColParts:          make(map[string]*colpart.Partition),
		Htables:           make(map[string]map[string]*dstruct.HashTable),
		MainLoop:          make(chan *Task, 100)}
	// Create server socket
	os.Remove(srv.ServerSock)
	srv.Listener, err = net.Listen("unix", srv.ServerSock)
	if err != nil {
		return
	}
	tdlog.Printf("Rank %d of %d is listening on %s", rank, totalRank, srv.ServerSock)
	// Accept incoming connections
	go func() {
		for {
			conn, err := srv.Listener.Accept()
			if err != nil {
				panic(err)
			}
			tdlog.Printf("Rank %d has an incoming connection", rank)
			// Process commands from incoming connection
			go CmdLoop(srv, &conn)
		}
	}()
	// Contact other ranks (after 2 seconds delay)
	time.Sleep(2 * time.Second)
	for i := 0; i < totalRank; i++ {
		if i == rank {
			continue
		}
		rankSockFile := path.Join(workingDir, strconv.Itoa(i))
		var conn net.Conn
		conn, err = net.Dial("unix", rankSockFile)
		if err != nil {
			return
		}
		srv.InterRank[i] = &conn
		srv.InterRankFeedback[i] = make(chan interface{}, 1)
		tdlog.Printf("Communication has been established between rank %d and %d on %s", rank, i, rankSockFile)
	}
	// Open my partition of the database
	if err = srv.Reload(); err != nil {
		return
	}
	return
}

// Start task worker
func (server *Server) Start() {
	defer os.Remove(server.ServerSock)
	for {
		task := <-server.MainLoop
		(task.Ret) <- task.Fun(task.Input)
	}
}

// Submit a task to the server and wait till its completion.
func (server *Server) Submit(task *Task) interface{} {
	server.MainLoop <- task
	return <-(task.Ret)
}

func (srv *Server) Shutdown() {
	srv.FlushAll(nil)
	os.Remove(srv.ServerSock)
	os.Exit(0)
}

func CmdLoop(srv *Server, conn *net.Conn) {
	resp := make(chan interface{}, 1)
	in := bufio.NewReader(*conn)
	out := bufio.NewWriter(*conn)

	// Helper functions for formulating server response
	AckOrErr := func(task *Task) {
		if err := srv.Submit(task); err == nil {
			out.WriteString(ACK)
		} else {
			if _, err2 := out.WriteString(fmt.Sprint(err)); err2 != nil {
				panic(err)
			}
		}
		out.WriteRune('\n')
		if err := out.Flush(); err != nil {
			panic(err)
		}
	}
	StrOrErr := func(task *Task) {
		ret := srv.Submit(task)
		switch ret.(type) {
		case string:
			out.WriteString(ret.(string))
		case error:
			out.WriteString(fmt.Sprint(ret))
		}
		out.WriteRune('\n')
		if err := out.Flush(); err != nil {
			panic(err)
		}
	}

	// Read commands from the connection, interpret them and execute them on the server loop
	for {
		cmd, err := in.ReadString(byte('\n'))
		if err != nil {
			tdlog.Printf("Connection is closed")
			return
		}
		cmd = cmd[0 : len(cmd)-1] // remove new-line suffix
		tdlog.Printf("CMD: %s", cmd)
		// Interpret commands which do not use parameters
		switch cmd {
		case FLUSH_ALL:
			AckOrErr(&Task{Ret: resp, Fun: srv.FlushAll})
		case SHUTDOWN:
			srv.Shutdown()
		default:
			// Interpret commands which Use parameters
			params := strings.SplitN(cmd, " ", 1+4) // there are at most 4 parameters used by any command
			switch params[0] {
			case COL_CREATE:
				AckOrErr(&Task{Ret: resp, Input: params, Fun: srv.ColCreate})
			case COL_ALL:
				StrOrErr(&Task{Ret: resp, Input: params, Fun: srv.ColAll})
			}
		}
	}
}
