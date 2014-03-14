/* Server structure and command loop. */
package network

import (
	"bufio"
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

	// Collection document manipulation with index updates
	COL_INSERT        = "cin"   // cin <col_name> <json_str>
	COL_GET           = "cgt"   // cgt <col_name> <id>
	COL_UPDATE        = "cup"   // cup <col_name> <id> <json_str>
	COL_UPDATE_NO_IDX = "cupni" // cupni <col_name> <id> <json_str>
	COL_DELETE        = "cde"   // cde <col_name> <id>
	COL_DELETE_NO_IDX = "cdeni" // cdeni <col_name> <id>

	// Document CRUD (no index update)
	DOC_INSERT = "din" // din <col_name> <json_str>
	DOC_GET    = "dgt" // dgt <col_name> <id>
	DOC_UPDATE = "dup" // dup <col_name> <id> <json_str>
	DOC_DELETE = "dde" // dde <col_name> <id>

	// Index entry manipulation
	HT_PUT    = "hpt" // hpt <col_name> <idx_name> <key> <val>
	HT_GET    = "hgt" // hgt <col_name> <idx_name> <key> <limit>
	HT_DELETE = "hde" // hde <col_name> <idx_name> <key> <val>

	// Other
	RELOAD    = "reload"
	FLUSH_ALL = "flush"
	SHUTDOWN  = "shutdown"
	PING      = "ping"    // for testing
	PING1     = "ping1"   // for testing
	PING_JSON = "pingjs"  // for testing
	PING_ERR  = "pingerr" // for testing
	// General response
	ACK = "OK"   // Acknowledgement
	ERR = "ERR " // Bad request/server error (mind the space)
)

const (
	INTER_RANK_CONN_RETRY = 20
	RETRY_EVERY           = 100 // milliseconds
)

// Tasks are queued on a server and executed one by one
type Task struct {
	Ret   chan interface{}           // Signal of function completion
	Input []string                   // Task function input parameter
	Fun   func([]string) interface{} // Task (function) with a return value type
}

// Server state and structures.
type Server struct {
	TempDir, DBDir  string // Working directory and DB directory
	ServerSock      string // Server socket file name
	Rank, TotalRank int    // Rank of current process; total number of processes
	// Schema information
	SchemaUpdateInProgress bool                  // Whether schema change is happening
	ColNumParts            map[string]int        // Collection name -> number of partitions
	ColIndexPathStr        map[string][]string   // Collection name -> indexed paths
	ColIndexPath           map[string][][]string // Collection name -> indexed path segments
	// My partition
	ColParts    map[string]*colpart.Partition            // Collection name -> partition
	Htables     map[string]map[string]*dstruct.HashTable // Collection name -> index name -> hash table
	Listener    net.Listener                             // This server socket
	InterRank   []*Client                                // Inter-rank communication connection
	MainLoop    chan *Task                               // Task loop
	ConnCounter int
}

// Start a new server.
func NewServer(rank, totalRank int, dbDir, tempDir string) (srv *Server, err error) {
	// It is very important for both client and server to initialize random seed
	rand.Seed(time.Now().UnixNano())
	if rank >= totalRank {
		panic("rank >= totalRank - should never happen")
	}
	// Create both database and working directories
	if err = os.MkdirAll(dbDir, 0700); err != nil {
		return
	}
	if err = os.MkdirAll(tempDir, 0700); err != nil {
		return
	}
	srv = &Server{Rank: rank, TotalRank: totalRank,
		ServerSock: path.Join(tempDir, strconv.Itoa(rank)),
		TempDir:    tempDir, DBDir: dbDir,
		InterRank:              make([]*Client, totalRank),
		SchemaUpdateInProgress: true,
		MainLoop:               make(chan *Task, 100)}
	// Create server socket
	os.Remove(srv.ServerSock)
	srv.Listener, err = net.Listen("unix", srv.ServerSock)
	if err != nil {
		return
	}
	// Start accepting incoming connections
	go func() {
		for {
			conn, err := srv.Listener.Accept()
			if err != nil {
				panic(err)
			}
			// Process commands from incoming connection
			go CmdLoop(srv, &conn)
		}
	}()
	// Establish inter-rank communications (including a connection to myself)
	for i := 0; i < totalRank; i++ {
		for retry := 0; retry < INTER_RANK_CONN_RETRY; retry++ {
			if srv.InterRank[i], err = NewClient(tempDir, i); err == nil {
				break
			} else {
				time.Sleep(RETRY_EVERY * time.Millisecond)
			}
		}
	}
	// Open my partition of the database
	if err2 := srv.Reload(nil); err2 != nil {
		return nil, err2.(error)
	}
	tdlog.Printf("Rank %d: Initialization completed, listening on %s", rank, srv.ServerSock)
	return
}

// Start task worker
func (server *Server) Start() {
	defer os.Remove(server.ServerSock)
	for {
		task := <-server.MainLoop
		for server.SchemaUpdateInProgress {
			time.Sleep(RETRY_EVERY * time.Millisecond)
		}
		(task.Ret) <- task.Fun(task.Input)
	}
}

// Submit a task to the server and wait till its completion.
func (server *Server) Submit(task *Task) interface{} {
	server.MainLoop <- task
	return <-(task.Ret)
}

// Broadcast a message to all other servers, return true on success.
func (srv *Server) BroadcastAway(line string, consumeResp bool, onErrResume bool) bool {
	for i, rank := range srv.InterRank {
		if i == srv.Rank {
			continue
		}
		if !rank.writeAway(line, consumeResp) && !onErrResume {
			return false
		}
	}
	return true
}

// Shutdown server and delete domain socket file.
func (srv *Server) Shutdown() {
	srv.BroadcastAway(SHUTDOWN, false, true)
	srv.FlushAll(nil)
	os.Remove(srv.ServerSock)
	tdlog.Printf("Rank %d: Shutdown upon client request", srv.Rank)
	os.Exit(0)
}

// Process commands from the client.
func CmdLoop(srv *Server, conn *net.Conn) {
	resp := make(chan interface{}, 1)
	in := bufio.NewReader(*conn)
	out := bufio.NewWriter(*conn)

	// Read commands from the connection, interpret them and execute them on the server loop
	for {
		cmd, err := in.ReadString(byte('\n'))
		if err != nil {
			return
		}
		cmd = cmd[0 : len(cmd)-1] // remove new-line suffix
		tdlog.Printf("Rank %d: Received %s", srv.Rank, cmd)
		// Interpret commands which do not use parameters
		switch cmd {
		case PING:
			if err = srv.ackOrErr(&Task{Ret: resp, Fun: srv.Ping}, out); err != nil {
				return
			}
		case PING1:
			if err = srv.uint64OrErr(&Task{Ret: resp, Fun: srv.Ping1}, out); err != nil {
				return
			}
		case PING_JSON:
			if err = srv.jsonOrErr(&Task{Ret: resp, Fun: srv.PingJS}, out); err != nil {
				return
			}
		case PING_ERR:
			if err = srv.ackOrErr(&Task{Ret: resp, Fun: srv.PingErr}, out); err != nil {
				return
			}
		case RELOAD:
			if err = srv.ackOrErr(&Task{Ret: resp, Fun: srv.Reload}, out); err != nil {
				return
			}
		case FLUSH_ALL:
			if err = srv.ackOrErr(&Task{Ret: resp, Fun: srv.FlushAll}, out); err != nil {
				return
			}
		case SHUTDOWN:
			srv.Shutdown()
		default:
			// Interpret parameterised commands
			params := strings.SplitN(cmd, " ", 1+4) // there are at most 4 parameters used by any command
			switch params[0] {
			// Collection management
			case COL_CREATE:
				if err = srv.ackOrErr(&Task{Ret: resp, Input: params, Fun: srv.ColCreate}, out); err != nil {
					return
				}
			case COL_ALL:
				if err = srv.jsonOrErr(&Task{Ret: resp, Input: params, Fun: srv.ColAll}, out); err != nil {
					return
				}
			case COL_RENAME:
				if err = srv.ackOrErr(&Task{Ret: resp, Input: params, Fun: srv.ColRename}, out); err != nil {
					return
				}
			case COL_DROP:
				if err = srv.ackOrErr(&Task{Ret: resp, Input: params, Fun: srv.ColDrop}, out); err != nil {
					return
				}
			// Index management
			case IDX_CREATE:
				if err = srv.ackOrErr(&Task{Ret: resp, Input: params, Fun: srv.IdxCreate}, out); err != nil {
					return
				}
			case IDX_ALL:
				if err = srv.jsonOrErr(&Task{Ret: resp, Input: params, Fun: srv.IdxAll}, out); err != nil {
					return
				}
			case IDX_DROP:
				if err = srv.ackOrErr(&Task{Ret: resp, Input: params, Fun: srv.IdxDrop}, out); err != nil {
					return
				}
			// Document manipulation including index updates
			case COL_INSERT:
				if err = srv.uint64OrErr(&Task{Ret: resp, Input: params, Fun: srv.ColInsert}, out); err != nil {
					return
				}
			case COL_GET:
				if err = srv.jsonOrErr(&Task{Ret: resp, Input: params, Fun: srv.ColGet}, out); err != nil {
					return
				}
			case COL_UPDATE:
				if err = srv.ackOrErr(&Task{Ret: resp, Input: params, Fun: srv.ColUpdate}, out); err != nil {
					return
				}
			case COL_UPDATE_NO_IDX:
				if err = srv.ackOrErr(&Task{Ret: resp, Input: params, Fun: srv.ColUpdateNoIdx}, out); err != nil {
					return
				}
			case COL_DELETE:
				if err = srv.ackOrErr(&Task{Ret: resp, Input: params, Fun: srv.ColDelete}, out); err != nil {
					return
				}
			case COL_DELETE_NO_IDX:
				if err = srv.ackOrErr(&Task{Ret: resp, Input: params, Fun: srv.ColDeleteNoIdx}, out); err != nil {
					return
				}
			// Document CRUD (no index update)
			case DOC_INSERT:
				if err = srv.strOrErr(&Task{Ret: resp, Input: params, Fun: srv.DocInsert}, out); err != nil {
					return
				}
			case DOC_GET:
				if err = srv.strOrErr(&Task{Ret: resp, Input: params, Fun: srv.DocGet}, out); err != nil {
					return
				}
			case DOC_UPDATE:
				if err = srv.strOrErr(&Task{Ret: resp, Input: params, Fun: srv.DocUpdate}, out); err != nil {
					return
				}
			case DOC_DELETE:
				if err = srv.ackOrErr(&Task{Ret: resp, Input: params, Fun: srv.DocDelete}, out); err != nil {
					return
				}
			// Index entry (hash table) manipulation
			case HT_PUT:
				if err = srv.ackOrErr(&Task{Ret: resp, Input: params, Fun: srv.HTPut}, out); err != nil {
					return
				}
			case HT_GET:
				if err = srv.strOrErr(&Task{Ret: resp, Input: params, Fun: srv.HTGet}, out); err != nil {
					return
				}
			case HT_DELETE:
				if err = srv.ackOrErr(&Task{Ret: resp, Input: params, Fun: srv.HTDelete}, out); err != nil {
					return
				}
			}
		}
	}
}
