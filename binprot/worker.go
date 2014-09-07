// The IO loop for serving an incoming connection.
package binprot

import (
	"bufio"
	"fmt"
	"github.com/HouzuoGuo/tiedot/db"
	"github.com/HouzuoGuo/tiedot/tdlog"
	"net"
	"sync/atomic"
)

const (
	// Status replies
	C_OK         = 0
	C_ERR        = 1
	C_ERR_SCHEMA = 2
	C_ERR_MAINT  = 3

	// Command record structure
	C_US = 31
	C_RS = 30

	// Document commands
	C_DOC_INSERT = 11
	C_DOC_READ   = 12
	C_DOC_UPDATE = 13
	C_DOC_DELETE = 14

	// Index commands
	C_HT_PUT    = 21
	C_HT_GET    = 22
	C_HT_REMOVE = 23

	// Maintenance commands
	C_RELOAD      = 91
	C_SHUTDOWN    = 92
	C_PING        = 93
	C_GO_MAINT    = 95
	C_LEAVE_MAINT = 96
)

// Close and reopen database.
func (srv *BinProtSrv) reload() (err error) {
	if err = srv.db.Close(); err != nil {
		return
	}
	srv.db, err = db.OpenDB(srv.dbPath)
	return
}

// The IO loop of serving an incoming connection. Block until the connection is closed or server shuts down.
func (srv *BinProtSrv) Serve(conn net.Conn) {
	clientID := atomic.AddInt64(&srv.clientIDSeq, 1)
	in := bufio.NewReader(conn)
	out := bufio.NewWriter(conn)

	var lastIOErr error

	for {
		if lastIOErr != nil {
			tdlog.Noticef("Lost connection to client %d: %v", clientID, lastIOErr)
			return
		}

		// Read a command from client
		cmd, params, err := SrvReadCmd(in)
		fmt.Println("CMD", cmd, params, err)
		if err != nil {
			tdlog.Noticef("Lost connection to client %d: %v", clientID, err)
			return
		}

		// Is server in maintenance mode?
		if atomic.LoadInt64(&srv.maintBy) != clientID {
			SrvAnsErr(out, C_ERR_MAINT)
			continue
		}

		// Process the command
		switch cmd {
		case C_GO_MAINT:
			if atomic.CompareAndSwapInt64(&srv.maintBy, 0, clientID) {
				lastIOErr = SrvAnsOK(out)
			} else {
				SrvAnsErr(out, C_ERR_MAINT)
			}
		case C_LEAVE_MAINT:
			if atomic.CompareAndSwapInt64(&srv.maintBy, clientID, 0) {
				lastIOErr = SrvAnsOK(out)
			} else {
				SrvAnsErr(out, C_ERR)
			}
		case C_RELOAD:
			if err := srv.reload(); err == nil {
				lastIOErr = SrvAnsOK(out)
			} else {
				panic(err)
			}
		case C_PING:
			SrvAnsOK(out)
		case C_SHUTDOWN:
			SrvAnsOK(out)
			srv.Shutdown()
			return
		}
	}
}
