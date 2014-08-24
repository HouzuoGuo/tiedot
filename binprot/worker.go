// The IO loop for serving an incoming connection.
package binprot

import (
	"bufio"
	"errors"
	"fmt"
	"github.com/HouzuoGuo/tiedot/db"
	"github.com/HouzuoGuo/tiedot/tdlog"
	"net"
)

const (
	C_DOC_INSERT = 11
	C_DOC_READ   = 12
	C_DOC_UPDATE = 13
	C_DOC_DELETE = 14

	C_HT_PUT    = 21
	C_HT_GET    = 22
	C_HT_REMOVE = 23

	C_RELOAD   = 91
	C_SHUTDOWN = 92
	C_PING     = 93
	C_PING_ERR = 94

	C_US  = 31
	C_RS  = 30
	C_OK  = 0
	C_ERR = 1
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
	in := bufio.NewReader(conn)
	out := bufio.NewWriter(conn)

	var lastIOErr error

	// Answer C_OK or C_ERR according to outcome of the function execution.
	okOrErr := func(fun func() error) {
		funErr := fun()
		if funErr == nil {
			lastIOErr = SrvAnsOK(out)
		} else {
			lastIOErr = SrvAnsErr(out, funErr.Error())
		}
	}

	for {
		if lastIOErr != nil {
			tdlog.Noticef("Lost connection to client: %v", lastIOErr)
		}
		cmd, params, err := SrvReadCmd(in)
		fmt.Println("CMD", cmd, params, err)
		if err != nil {
			tdlog.Noticef("Lost connection to client: %v", err)
			return
		}
		if srv.db == nil {
			okOrErr(func() error {
				return errors.New("Database is not opened yet")
			})
		}
		switch cmd {
		case C_RELOAD:
			okOrErr(srv.reload)
		case C_PING:
			okOrErr(func() error {
				return nil
			})
		case C_PING_ERR:
			okOrErr(func() error {
				return errors.New("this is an error")
			})
		}
	}
}
