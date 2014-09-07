// Binary protocol over IPC - server.

package binprot

import (
	"github.com/HouzuoGuo/tiedot/db"
	"github.com/HouzuoGuo/tiedot/tdlog"
	"net"
	"os"
	"path"
	"strconv"
)

const (
	SOCK_FILE = "sock" // name of server rank's Unix socket file
)

// Bin protocol server opens a database of its rank, and listens on a Unix domain socket.
type BinProtSrv struct {
	myRank, nProcs              int
	workspace, dbPath, sockPath string
	srvSock                     net.Listener
	db                          *db.DB
	clientIDSeq                 int64
	maintBy                     int64
}

// Create a server, but do not yet start serving incoming connections.
func NewServer(myRank, nProcs int, workspace string) (srv *BinProtSrv) {
	srv = &BinProtSrv{
		myRank:      myRank,
		nProcs:      nProcs,
		workspace:   workspace,
		dbPath:      path.Join(workspace, strconv.Itoa(myRank)),
		sockPath:    path.Join(workspace, strconv.Itoa(myRank), SOCK_FILE),
		clientIDSeq: 1,
		maintBy:     0}

	return srv
}

// Serve incoming connections. Block until server is told to shutdown.
func (srv *BinProtSrv) Run() (err error) {
	os.Remove(srv.sockPath)
	if srv.db, err = db.OpenDB(srv.dbPath); err != nil {
		return
	} else if srv.srvSock, err = net.Listen("unix", srv.sockPath); err != nil {
		return
	}
	for {
		conn, err := srv.srvSock.Accept()
		if err != nil {
			tdlog.Noticef("Server is closing down: %v", err)
			return nil
		}
		go srv.Serve(conn)
	}
}

// Stop serving new/existing connections and shut server down.
func (srv *BinProtSrv) Shutdown() {
	if err := srv.srvSock.Close(); err != nil {
		tdlog.Noticef("Failed to close server socket: %v", err)
	}
}
