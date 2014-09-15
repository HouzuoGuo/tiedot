// Binary protocol over unix domain socket - server.

package binprot

import (
	"bufio"
	"github.com/HouzuoGuo/tiedot/data"
	"github.com/HouzuoGuo/tiedot/db"
	"github.com/HouzuoGuo/tiedot/tdlog"
	"net"
	"os"
	"path"
	"strconv"
	"sync"
	"sync/atomic"
)

const (
	SOCK_FILE = "sock" // name of server rank's Unix socket file
)

// Bin protocol server opens a database of its rank, and listens on a Unix domain socket.
type BinProtSrv struct {
	rank, nProcs                int
	workspace, dbPath, sockPath string
	srvSock                     net.Listener
	db                          *db.DB
	colLookup                   map[int32]*db.Col
	colNameLookup               map[string]int32
	htLookup                    map[int32]*data.HashTable
	htNameLookup                map[string]int32
	clientIDSeq, maintByClient  int64
	rev                         uint32
	oneAtATime                  *sync.Mutex
	shutdown                    bool
}

// Serve incoming connection.
type BinProtWorker struct {
	srv     *BinProtSrv
	id      int64
	in      *bufio.Reader
	out     *bufio.Writer
	lastErr error
}

// Create a server, but do not yet start serving incoming connections.
func NewServer(rank, nProcs int, workspace string) (srv *BinProtSrv) {
	srv = &BinProtSrv{
		rank:          rank,
		nProcs:        nProcs,
		workspace:     workspace,
		dbPath:        path.Join(workspace, strconv.Itoa(rank)),
		sockPath:      path.Join(workspace, strconv.Itoa(rank), SOCK_FILE),
		colLookup:     make(map[int32]*db.Col),
		htLookup:      make(map[int32]*data.HashTable),
		clientIDSeq:   0,
		maintByClient: 0,
		rev:           0,
		oneAtATime:    new(sync.Mutex),
		shutdown:      false}

	return srv
}

// Serve incoming connections. Block until server is told to shutdown.
func (srv *BinProtSrv) Run() (err error) {
	os.Remove(srv.sockPath)
	srv.reload()
	if srv.srvSock, err = net.Listen("unix", srv.sockPath); err != nil {
		return
	}
	tdlog.Noticef("Server %d: is listening on %s", srv.rank, srv.sockPath)
	for {
		conn, err := srv.srvSock.Accept()
		if err != nil {
			tdlog.Noticef("Server %d: is closing down - %v", srv.rank, err)
			return nil
		}
		worker := &BinProtWorker{
			srv: srv,
			id:  atomic.AddInt64(&srv.clientIDSeq, 1),
			in:  bufio.NewReader(conn),
			out: bufio.NewWriter(conn)}
		go worker.Run()
	}
}

// To save bandwidth, both client and server refer collections and indexes by an int32.
func mkSchemaLookupTables(dbInstance *db.DB) (colLookup map[int32]*db.Col, colNameLookup map[string]int32, htLookup map[int32]*data.HashTable, htNameLookup map[string]int32) {
	colLookup = make(map[int32]*db.Col)
	colNameLookup = make(map[string]int32)
	htLookup = make(map[int32]*data.HashTable)
	htNameLookup = make(map[string]int32)
	seq := 0
	for _, colName := range dbInstance.AllCols() {
		col := dbInstance.Use(colName)
		colLookup[int32(seq)] = col
		colNameLookup[colName] = int32(seq)
		seq++
		for _, idxName := range col.AllIndexesJointPaths() {
			htLookup[int32(seq)] = col.BPUseHT(idxName)
			htNameLookup[idxName] = int32(seq)
			seq++
		}
	}
	return
}

// Close and reopen database.
func (srv *BinProtSrv) reload() {
	var err error
	if srv.db != nil {
		if err = srv.db.Close(); err != nil {
			tdlog.Noticef("Server %d: failed to close DB before reloading - %v", srv.rank, err)
		}
	}
	if srv.db, err = db.OpenDB(srv.dbPath); err != nil {
		panic(err)
	}
	srv.rev++
	// Support numeric lookup of collections and hash tables
	srv.colLookup, srv.colNameLookup, srv.htLookup, srv.htNameLookup = mkSchemaLookupTables(srv.db)
}

// Stop serving new/existing connections and shut server down.
func (srv *BinProtSrv) Shutdown() {
	if err := srv.srvSock.Close(); err != nil {
		tdlog.Noticef("Server %d: failed to close server socket - %v", srv.rank, err)
	}
	srv.shutdown = true
}
