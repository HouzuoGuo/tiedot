// The IO loop for serving an incoming connection.
package binprot

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/HouzuoGuo/tiedot/tdlog"
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

// Server reads a "CMD-REV-PARAM-US-PARAM-RS" command sent by client.
func (worker *BinProtWorker) readCmd() (cmd byte, rev uint32, params [][]byte, err error) {
	cmdRev := make([]byte, 5)
	if _, err = worker.in.Read(cmdRev); err != nil {
		return
	}
	cmd = cmdRev[0]
	rev = binary.LittleEndian.Uint32(cmdRev[1:5])
	record, err := worker.in.ReadSlice(C_RS)
	record = record[0 : len(record)-1]
	if err != nil {
		return
	}
	params = bytes.Split(record, []byte{C_US})
	return
}

// Server answers "OK-INFO-US-INFO-RS".
func (worker *BinProtWorker) ansOK(moreInfo ...[]byte) {
	if err := worker.out.WriteByte(C_OK); err != nil {
		worker.lastErr = err
		return
	}
	for _, more := range moreInfo {
		if _, err := worker.out.Write(more); err != nil {
			worker.lastErr = err
			return
		} else if err := worker.out.WriteByte(C_US); err != nil {
			worker.lastErr = err
			return
		}
	}
	if err := worker.out.WriteByte(C_RS); err != nil {
		worker.lastErr = err
		return
	} else if err := worker.out.Flush(); err != nil {
		worker.lastErr = err
		return
	}
}

// Server answers "ERR-INFO-RS".
func (worker *BinProtWorker) ansErr(errCode byte, moreInfo []byte) {
	if err := worker.out.WriteByte(errCode); err != nil {
		worker.lastErr = err
		return
	} else if _, err := worker.out.Write(moreInfo); err != nil {
		worker.lastErr = err
		return
	} else if err := worker.out.WriteByte(C_RS); err != nil {
		worker.lastErr = err
		return
	} else if err := worker.out.Flush(); err != nil {
		worker.lastErr = err
		return
	}
}

// The IO loop serving an incoming connection. Block until the connection is closed or server shuts down.
func (worker *BinProtWorker) Run() {
	tdlog.Noticef("Server %d: running worker for client %d", worker.srv.rank, worker.id)
	for {
		if worker.lastErr != nil {
			tdlog.Noticef("Server %d: lost connection to client %d", worker.srv.rank, worker.id)
			return
		}
		// Read a command from client
		cmd, clientRev, params, err := worker.readCmd()
		fmt.Sprint("CMD", cmd, params, err)
		if err != nil {
			// Client has disconnected
			tdlog.Noticef("Server %d: lost connection with client %d - %v", worker.srv.rank, worker.id, err)
			return
		}
		worker.srv.oneAtATime.Lock()
		if clientRev != worker.srv.rev {
			// Check client revision
			tdlog.Noticef("Server %d: telling client %d (rev %d) to refresh schema revision to match %d", worker.srv.rank, worker.id, clientRev, worker.srv.rev)
			mySchema := make([]byte, 4)
			binary.LittleEndian.PutUint32(mySchema, worker.srv.rev)
			worker.ansErr(C_ERR_SCHEMA, mySchema)
		} else if worker.srv.maintByClient != 0 && worker.srv.maintByClient != worker.id {
			// Check server maintenance access
			worker.ansErr(C_ERR_MAINT, []byte{})
		} else {
			// Process the command
			switch cmd {
			case C_GO_MAINT:
				worker.srv.maintByClient = worker.id
				worker.ansOK()
			case C_LEAVE_MAINT:
				if worker.srv.maintByClient == worker.id {
					worker.srv.maintByClient = 0
					worker.srv.reload()
					worker.ansOK()
				} else {
					worker.ansErr(C_ERR_MAINT, []byte{})
				}
			case C_PING:
				worker.ansOK()
			case C_SHUTDOWN:
				worker.ansOK()
				worker.srv.Shutdown()
				return
			}
		}
		worker.srv.oneAtATime.Unlock()
	}
}
