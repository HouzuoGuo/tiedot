// Binary protocol over unix domain socket - server messaging and IO loop.

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
	"strings"
	"sync"
	"sync/atomic"
)

const (
	SOCK_FILE_SUFFIX = "_sock" // name of server rank's Unix socket file
)

// Bin protocol server opens a database of its rank, and listens on a Unix domain socket.
type BinProtSrv struct {
	rank, nProcs                int
	workspace, dbPath, sockPath string
	srvSock                     net.Listener
	db                          *db.DB
	colLookup                   map[int32]*db.Col
	htLookup                    map[int32]*data.HashTable
	clientIDSeq, maintByClient  uint64
	rev                         uint32
	opLock                      *sync.Mutex
	shutdown                    bool
	pendingUpdates              int64
}

// Serve incoming connection.
type BinProtWorker struct {
	srv     *BinProtSrv
	id      uint64
	in      *bufio.Reader
	out     *bufio.Writer
	lastErr error
}

// Create a server, but do not yet start serving incoming connections.
func NewServer(rank, nProcs int, workspace string) (srv *BinProtSrv) {
	srv = &BinProtSrv{
		rank:           rank,
		nProcs:         nProcs,
		workspace:      workspace,
		dbPath:         path.Join(workspace, strconv.Itoa(rank)),
		sockPath:       path.Join(workspace, strconv.Itoa(rank)+SOCK_FILE_SUFFIX),
		clientIDSeq:    0,
		maintByClient:  0,
		rev:            0,
		opLock:         new(sync.Mutex),
		shutdown:       false,
		pendingUpdates: 0}

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
			id:  atomic.AddUint64(&srv.clientIDSeq, 1),
			in:  bufio.NewReader(conn),
			out: bufio.NewWriter(conn)}
		go worker.Run()
	}
}

// To save bandwidth, both client and server refer collections and indexes by an int32 "ID", instead of using their string names.
func mkSchemaLookupTables(dbInstance *db.DB) (colLookup map[int32]*db.Col,
	colNameLookup map[string]int32,
	htLookup map[int32]*data.HashTable,
	indexPaths map[int32]map[int32][]string) {

	colLookup = make(map[int32]*db.Col)
	colNameLookup = make(map[string]int32)
	htLookup = make(map[int32]*data.HashTable)
	indexPaths = make(map[int32]map[int32][]string)

	// Both server and client run the same version of Go, therefore the order in which map keys are traversed is the same.
	seq := 0
	for _, colName := range dbInstance.AllCols() {
		col := dbInstance.Use(colName)
		colID := int32(seq)
		colLookup[colID] = col
		colNameLookup[colName] = colID
		indexPaths[colID] = make(map[int32][]string)
		seq++
		for _, idxPath := range col.AllIndexes() {
			htLookup[int32(seq)] = col.BPUseHT(strings.Join(idxPath, db.INDEX_PATH_SEP))
			indexPaths[colID][int32(seq)] = idxPath
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
	srv.colLookup, _, srv.htLookup, _ = mkSchemaLookupTables(srv.db)
}

// Stop serving new/existing connections and shut server down.
func (srv *BinProtSrv) Shutdown() {
	if err := srv.srvSock.Close(); err != nil {
		tdlog.Noticef("Server %d: failed to close server socket - %v", srv.rank, err)
	}
	srv.shutdown = true
}
