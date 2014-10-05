// The IO loop for serving an incoming connection.
package binprot

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/HouzuoGuo/tiedot/tdlog"
)

// Server reads a "CMD-REV-PARAM-US-PARAM-RS" command sent by client.
func (worker *BinProtWorker) readCmd() (cmd byte, rev uint32, params [][]byte, err error) {
	cmdRev := make([]byte, 5)
	if _, err = worker.in.Read(cmdRev); err != nil {
		return
	}
	cmd = cmdRev[0]
	rev = binary.LittleEndian.Uint32(cmdRev[1:5])
	record, err := worker.in.ReadSlice(REC_END)
	record = record[0 : len(record)-1]
	if err != nil {
		return
	}
	params = bytes.Split(record, []byte{REC_PARAM})
	return
}

// Server answers "OK-INFO-US-INFO-RS".
func (worker *BinProtWorker) ansOK(moreInfo ...[]byte) {
	if err := worker.out.WriteByte(R_OK); err != nil {
		worker.lastErr = err
		return
	}
	for _, more := range moreInfo {
		if _, err := worker.out.Write(more); err != nil {
			worker.lastErr = err
			return
		} else if err := worker.out.WriteByte(REC_PARAM); err != nil {
			worker.lastErr = err
			return
		}
	}
	if err := worker.out.WriteByte(REC_END); err != nil {
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
	} else if err := worker.out.WriteByte(REC_END); err != nil {
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
		fmt.Println("Server read ", cmd, clientRev, params)
		if err != nil {
			// Client has disconnected
			tdlog.Noticef("Server %d: lost connection with client %d - %v", worker.srv.rank, worker.id, err)
			return
		} else if worker.srv.shutdown {
			// Server has/is shutting down
			worker.ansErr(R_ERR_DOWN, []byte{})
			return
		}
		worker.srv.opLock.Lock()
		if clientRev != worker.srv.rev {
			// Check client revision
			tdlog.Noticef("Server %d: telling client %d (rev %d) to refresh schema revision to match %d", worker.srv.rank, worker.id, clientRev, worker.srv.rev)
			mySchema := make([]byte, 4)
			binary.LittleEndian.PutUint32(mySchema, worker.srv.rev)
			worker.ansErr(R_ERR_SCHEMA, mySchema)
		} else if worker.srv.maintByClient != 0 && worker.srv.maintByClient != worker.id {
			// Check server maintenance access
			worker.ansErr(R_ERR_MAINT, []byte{})
		} else {
			// Process the command
			switch cmd {
			case C_DOC_INSERT:
				colID := int32(binary.LittleEndian.Uint32(params[0]))
				docID := binary.LittleEndian.Uint64(params[1])
				doc := params[2]
				if err := worker.srv.colLookup[colID].BPLockAndInsert(docID, doc); err == nil {
					worker.ansOK()
				} else {
					worker.ansErr(R_ERR, []byte(err.Error()))
				}
			case C_DOC_UNLOCK:
				colID := int32(binary.LittleEndian.Uint32(params[0]))
				docID := binary.LittleEndian.Uint64(params[1])
				worker.srv.colLookup[colID].BPUnlock(docID)
				worker.ansOK()
			case C_GO_MAINT:
				worker.srv.maintByClient = worker.id
				worker.ansOK()
			case C_LEAVE_MAINT:
				if worker.srv.maintByClient == worker.id {
					worker.srv.maintByClient = 0
					worker.srv.reload()
					worker.ansOK()
				} else {
					worker.ansErr(R_ERR_MAINT, []byte{})
				}
			case C_PING:
				clientID := make([]byte, 8)
				binary.LittleEndian.PutUint64(clientID, uint64(worker.id))
				worker.ansOK(clientID)
			case C_SHUTDOWN:
				worker.srv.Shutdown()
				worker.ansOK()
			}
		}
		worker.srv.opLock.Unlock()
	}
}
