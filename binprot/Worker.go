package binprot

import (
	"bufio"
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
)

func (srv *BinProtSrv) Serve(conn net.Conn) {
	in := bufio.NewReader(conn)
	out := bufio.NewWriter(conn)
	for {
		cmd, err := in.ReadByte()
		if err != nil {
			tdlog.Noticef("Lost connection to client")
			return
		}
		switch cmd {
		case C_SHUTDOWN:
			srv.Shutdown()
			conn.Close()
			return
		}
		out.WriteByte(1)
	}
}
