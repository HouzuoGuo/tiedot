// DB sharding via IPC using a binary protocol - shard server structure.
package sharding

import (
	"bufio"
	"github.com/HouzuoGuo/tiedot/data"
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
type ShardServer struct {
	rank, nProcs                int
	workspace, dbPath, sockPath string
	srvSock                     net.Listener
	clientIDSeq, maintByClient  uint64
	opLock                      *sync.Mutex
	shutdown                    bool
	pendingTransactions         int64
	dbo                         *data.DBObjects
}

// Serve incoming connection.
type ShardServerWorker struct {
	srv                *ShardServer
	id                 uint64
	in                 *bufio.Reader
	out                *bufio.Writer
	sock               net.Conn
	pendingTransaction bool
	pendingMaintenance bool
	lastErr            error
}

// Create a server, but do not yet start serving incoming connections.
func NewServer(rank, nProcs int, workspace string) (srv *ShardServer) {
	return &ShardServer{
		rank:                rank,
		nProcs:              nProcs,
		workspace:           workspace,
		dbPath:              path.Join(workspace, strconv.Itoa(rank)),
		sockPath:            path.Join(workspace, strconv.Itoa(rank)+SOCK_FILE_SUFFIX),
		clientIDSeq:         0,
		maintByClient:       0,
		opLock:              new(sync.Mutex),
		shutdown:            false,
		pendingTransactions: 0}
}

// Serve incoming connections. Block until server is told to shutdown.
func (srv *ShardServer) Run() (err error) {
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
		worker := &ShardServerWorker{
			srv:                srv,
			id:                 atomic.AddUint64(&srv.clientIDSeq, 1),
			sock:               conn,
			in:                 bufio.NewReader(conn),
			out:                bufio.NewWriter(conn),
			pendingTransaction: false,
			pendingMaintenance: false}
		go worker.Run()
	}
}

// Close and reopen database.
func (srv *ShardServer) reload() {
	if err := srv.dbo.Reload(); err != nil {
		panic(err)
	}
	tdlog.Infof("Server %d: schema reloaded to revision %d", srv.rank, srv.dbo.GetCurrentRev())
}

func (srv *ShardServer) shutdown0() {
	if err := srv.srvSock.Close(); err != nil {
		tdlog.Noticef("Server %d: failed to close server socket - %v", srv.rank, err)
	}
	srv.shutdown = true
}

// Stop serving new/existing connections and shut server down.
func (srv *ShardServer) Shutdown() {
	srv.opLock.Lock()
	srv.shutdown0()
	srv.opLock.Unlock()
}
