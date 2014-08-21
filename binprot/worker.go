package binprot

import (
	"bufio"
	"fmt"
	"github.com/HouzuoGuo/tiedot/tdlog"
	"net"
)

const (
	C_COL_CREATE = 1
	C_COL_RENAME = 2
	C_COL_DROP   = 3

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

func (srv *BinProtSrv) Serve(conn net.Conn) {
	in := bufio.NewReader(conn)
	out := bufio.NewWriter(conn)
	for {
		cmd, params, err := SrvReadCmd(in)
		fmt.Println("CMD", cmd, params, err)
		if err != nil {
			tdlog.Noticef("Lost connection to client")
			return
		}
		switch cmd {
		case C_PING:
			if err = SrvAnsOK(out); err != nil {
				return
			}
		case C_PING_ERR:
			if err = SrvAnsErr(out, "this is an error"); err != nil {
				return
			}
		}
	}
}
