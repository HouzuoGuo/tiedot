// Binary protocol over unix domain socket - server messaging and IO loop.

package binprot

import (
	"bufio"
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
	SOCK_FILE_SUFFIX = "_sock" // name of server rank's Unix socket file
)

// Bin protocol server opens a database of its rank, and listens on a Unix domain socket.
type BinProtSrv struct {
	rank, nProcs                int
	workspace, dbPath, sockPath string
	srvSock                     net.Listener
	db                          *db.DB
	clientIDSeq, maintByClient  uint64
	opLock                      *sync.Mutex
	shutdown                    bool
	pendingTransactions         int64
	schema                      *Schema
}

// Serve incoming connection.
type BinProtWorker struct {
	srv                *BinProtSrv
	id                 uint64
	in                 *bufio.Reader
	out                *bufio.Writer
	pendingTransaction bool
	pendingMaintenance bool
	lastErr            error
}

// Create a server, but do not yet start serving incoming connections.
func NewServer(rank, nProcs int, workspace string) (srv *BinProtSrv) {
	return &BinProtSrv{
		rank:                rank,
		nProcs:              nProcs,
		workspace:           workspace,
		dbPath:              path.Join(workspace, strconv.Itoa(rank)),
		sockPath:            path.Join(workspace, strconv.Itoa(rank)+SOCK_FILE_SUFFIX),
		clientIDSeq:         0,
		maintByClient:       0,
		opLock:              new(sync.Mutex),
		shutdown:            false,
		pendingTransactions: 0,
		schema:              new(Schema)}
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
			srv:                srv,
			id:                 atomic.AddUint64(&srv.clientIDSeq, 1),
			in:                 bufio.NewReader(conn),
			out:                bufio.NewWriter(conn),
			pendingTransaction: false,
			pendingMaintenance: false}
		go worker.Run()
	}
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
	srv.schema.refresh(srv.db)
}

// Stop serving new/existing connections and shut server down.
func (srv *BinProtSrv) Shutdown() {
	if err := srv.srvSock.Close(); err != nil {
		tdlog.Noticef("Server %d: failed to close server socket - %v", srv.rank, err)
	}
	srv.shutdown = true
}
