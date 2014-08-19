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
	SOCK_FILE = "sock"
)

type BinProtSrv struct {
	myRank, nProcs              int
	workspace, dbPath, sockPath string
	srvSock                     net.Listener
	db                          *db.DB
}

func NewServer(myRank, nProcs int, workspace string) (srv *BinProtSrv) {
	srv = &BinProtSrv{
		myRank:    myRank,
		nProcs:    nProcs,
		workspace: workspace,
		dbPath:    path.Join(workspace, strconv.Itoa(myRank)),
		sockPath:  path.Join(workspace, strconv.Itoa(myRank), "sock")}
	return srv
}

func (srv *BinProtSrv) Start() (err error) {
	os.Remove(srv.sockPath)
	if srv.db, err = db.OpenDB(srv.dbPath); err != nil {
		return
	} else if srv.srvSock, err = net.Listen("unix", srv.sockPath); err != nil {
		return
	}
	for {
		conn, err := srv.srvSock.Accept()
		if err != nil {
			tdlog.Noticef("Accept failed on socket, error %v", err)
		}
		go srv.Serve(conn)
	}
}

func (srv *BinProtSrv) Shutdown() {
	if err := srv.srvSock.Close(); err != nil {
		tdlog.Noticef("Failed to close server socket: %v", err)
	}
	if err := os.Remove(srv.sockPath); err != nil {
		tdlog.Noticef("Failed to remove closed server socket: %v", err)
	}
}
