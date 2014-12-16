// The IO loop for serving an incoming connection.
package binprot

import (
	"fmt"
	"github.com/HouzuoGuo/tiedot/tdlog"
	"sync/atomic"
)

const (
	// Status reply from server
	R_OK         = 0
	R_ERR        = 1
	R_ERR_SCHEMA = 2
	R_ERR_MAINT  = 3
	R_ERR_DOWN   = 4

	// Client status - error (not a server reply)
	CLIENT_IO_ERR = 5

	// Document commands
	C_DOC_INSERT       = 11
	C_DOC_UNLOCK       = 12
	C_DOC_READ         = 13
	C_DOC_LOCK_READ    = 14
	C_DOC_UPDATE       = 15
	C_DOC_DELETE       = 16
	C_DOC_APPROX_COUNT = 17
	C_DOC_GET_PAGE     = 18

	// Index and query commands
	C_HT_PUT     = 21
	C_HT_GET     = 22
	C_HT_REMOVE  = 23
	C_QUERY_PRE  = 24
	C_QUERY_POST = 25

	// Maintenance commands
	C_RELOAD      = 91
	C_SHUTDOWN    = 92
	C_PING        = 93
	C_GO_MAINT    = 95
	C_LEAVE_MAINT = 96
)

// Server reads a command sent from a client.
func (worker *BinProtWorker) readCmd() (cmd byte, rev uint32, params [][]byte, err error) {
	cmd, paramsInclRev, err := readRec(worker.in)
	if err != nil {
		return
	}
	rev = Uint32(paramsInclRev[0])
	params = paramsInclRev[1:]
	return
}

// Server answers OK with extra params.
func (worker *BinProtWorker) ansOK(moreInfo ...[]byte) {
	if err := writeRec(worker.out, R_OK, moreInfo...); err != nil {
		worker.lastErr = err
	}
}

// Server answers an error with optionally more info.
func (worker *BinProtWorker) ansErr(errCode byte, moreInfo []byte) {
	if err := writeRec(worker.out, errCode, moreInfo); err != nil {
		worker.lastErr = err
	}
}

// Disconnect and clean after the client in case of IO/client error.
func (worker *BinProtWorker) disconnect() {
	tdlog.Noticef("Server %d: connection lost/disconnected from client %d due to error %v", worker.srv.rank, worker.id, worker.lastErr)
	if worker.pendingMaintenance {
		worker.srv.opLock.Lock()
		worker.srv.maintByClient = 0
		if !worker.srv.shutdown {
			worker.srv.reload()
		}
		worker.srv.opLock.Unlock()
	}
	if worker.pendingTransaction {
		atomic.AddInt64(&worker.srv.pendingTransactions, -1)
	}
}

// The IO loop serving an incoming connection. Block until the connection is closed or server shuts down.
func (worker *BinProtWorker) Run() {
	tdlog.Noticef("Server %d: running worker for client %d", worker.srv.rank, worker.id)
	for {
		if worker.lastErr != nil {
			worker.disconnect()
			return
		}
		// Read command and parameters from client
		cmd, clientRev, params, err := worker.readCmd()
		worker.lastErr = err
		if worker.lastErr != nil {
			worker.disconnect()
			return
		} else if worker.srv.shutdown {
			// Inform client that server has/is shutting down
			worker.ansErr(R_ERR_DOWN, []byte{})
			return
		}
		// The entire server process serves only one request at a time, governed by mutex.
		worker.srv.opLock.Lock()
		srvRev := worker.srv.schema.rev
		if clientRev != srvRev {
			// In case of revision mismatch, inform client to refresh its schema.
			worker.ansErr(R_ERR_SCHEMA, Buint32(srvRev))
		} else if worker.srv.maintByClient != 0 && worker.srv.maintByClient != worker.id {
			// In case of an ongoing maintenance operation, inform client to retry later.
			worker.ansErr(R_ERR_MAINT, []byte("Server is being maintained by another client"))
		} else {
			// Process the command
			switch cmd {
			case C_DOC_INSERT:
				// (Increase pending-transaction counter)
				// Insert and lock a document - collection ID, new document ID, serialized document content
				colID := Int32(params[0])
				docID := Uint64(params[1])
				doc := params[2]
				col, exists := worker.srv.schema.colLookup[colID]
				if worker.pendingTransaction {
					worker.ansErr(R_ERR, []byte("Client mishaved and asked for transaction twice"))
					worker.disconnect()
				} else if !exists {
					worker.ansErr(R_ERR_SCHEMA, []byte("Collection does not exist"))
				} else if err := col.BPLockAndInsert(docID, doc); err != nil {
					worker.ansErr(R_ERR, []byte(err.Error()))
				} else {
					worker.pendingTransaction = true
					atomic.AddInt64(&worker.srv.pendingTransactions, 1)
					worker.ansOK()
				}
			case C_DOC_UNLOCK:
				// (Decrease pending-transaction counter)
				// Unlock a document to allow further updates - collection ID, document ID
				colID := Int32(params[0])
				docID := Uint64(params[1])
				col, exists := worker.srv.schema.colLookup[colID]
				if !worker.pendingTransaction {
					worker.ansErr(R_ERR, []byte("Client mishaved and asked for ending transaction without starting one"))
					worker.disconnect()
				} else if exists {
					col.BPUnlock(docID)
					worker.pendingTransaction = false
					atomic.AddInt64(&worker.srv.pendingTransactions, -1)
					worker.ansOK()
				} else {
					worker.ansErr(R_ERR_SCHEMA, []byte("Collection does not exist"))
					worker.disconnect()
					return
				}
			case C_DOC_READ:
				// Read a document back to the client - collection ID, document ID
				colID := Int32(params[0])
				docID := Uint64(params[1])
				col, exists := worker.srv.schema.colLookup[colID]
				if !exists {
					worker.ansErr(R_ERR_SCHEMA, []byte("Collection does not exist"))
				} else if doc, err := col.BPRead(docID); err == nil {
					worker.ansOK(doc)
				} else {
					worker.ansErr(R_ERR, []byte(err.Error()))
				}
			case C_DOC_LOCK_READ:
				// (Increase pending-transaction counter)
				// Read a document back to the client, and lock the document for update - collection ID, document ID
				colID := Int32(params[0])
				docID := Uint64(params[1])
				col, exists := worker.srv.schema.colLookup[colID]
				if worker.pendingTransaction {
					worker.ansErr(R_ERR, []byte("Client mishaved and asked for transaction twice"))
					worker.disconnect()
				} else if !exists {
					worker.ansErr(R_ERR_SCHEMA, []byte("Collection does not exist"))
				} else if doc, err := col.BPLockAndRead(docID); err == nil {
					worker.pendingTransaction = true
					atomic.AddInt64(&worker.srv.pendingTransactions, 1)
					worker.ansOK(doc)
				} else {
					worker.ansErr(R_ERR, []byte(err.Error()))
				}
			case C_DOC_UPDATE:
				// Overwrite document content - collection ID, document ID, new document content
				colID := Int32(params[0])
				docID := Uint64(params[1])
				doc := params[2]
				col, exists := worker.srv.schema.colLookup[colID]
				if !exists {
					worker.ansErr(R_ERR_SCHEMA, []byte("Collection does not exist"))
				} else if err := col.BPUpdate(docID, doc); err == nil {
					worker.ansOK()
				} else {
					worker.ansErr(R_ERR, []byte(err.Error()))
				}
			case C_DOC_DELETE:
				// Delete a document - collection ID, document ID, new document
				colID := Int32(params[0])
				docID := Uint64(params[1])
				col, exists := worker.srv.schema.colLookup[colID]
				if exists {
					col.BPDelete(docID)
					worker.ansOK()
				} else {
					worker.ansErr(R_ERR_SCHEMA, []byte("Collection does not exist"))
				}
			case C_DOC_APPROX_COUNT:
				// Return approximate document count - collection ID
				colID := Int32(params[0])
				col, exists := worker.srv.schema.colLookup[colID]
				if exists {
					worker.ansOK(Buint64(col.ApproxDocCount()))
				} else {
					worker.ansErr(R_ERR_SCHEMA, []byte("Collection does not exist"))
				}
			case C_DOC_GET_PAGE:
				// Divide collection into roughly equally sized pages and return a page of documents - collectionID, page number, page total
				colID := Int32(params[0])
				page := Uint64(params[1])
				total := Uint64(params[2])
				col, exists := worker.srv.schema.colLookup[colID]
				if exists {
					approxCount := col.ApproxDocCount()
					// id1, doc1, id2, doc2, id3, doc3 ...
					resp := make([][]byte, 0, approxCount/total*page*2)
					col.ForEachDocInPage(page, total, func(id uint64, doc []byte) (moveOn bool) {
						resp = append(resp, Buint64(id), doc)
						return true
					})
					worker.ansOK(resp...)
				} else {
					worker.ansErr(R_ERR_SCHEMA, []byte("Collection does not exist"))
				}
			case C_HT_GET:
				// Lookup by key in a hash table - hash table ID, hash key, result limit
				htID := Int32(params[0])
				htKey := Uint64(params[1])
				limit := Uint64(params[2])
				ht, exists := worker.srv.schema.htLookup[htID]
				if exists {
					vals := ht.Get(htKey, limit)
					resp := make([][]byte, len(vals))
					for i := range vals {
						resp[i] = Buint64(vals[i])
					}
					worker.ansOK(resp...)
				} else {
					worker.ansErr(R_ERR_SCHEMA, []byte{})
				}
			case C_HT_PUT:
				// Put a new entry into hash table - hash table ID, hash key, hash value
				htID := Int32(params[0])
				htKey := Uint64(params[1])
				htVal := Uint64(params[2])
				ht, exists := worker.srv.schema.htLookup[htID]
				if exists {
					ht.Put(htKey, htVal)
					worker.ansOK()
				} else {
					worker.ansErr(R_ERR_SCHEMA, []byte{})
				}
			case C_HT_REMOVE:
				// Remove an entry from hash table - hash table ID, hash key, hash value
				htID := Int32(params[0])
				htKey := Uint64(params[1])
				htVal := Uint64(params[2])
				ht, exists := worker.srv.schema.htLookup[htID]
				if exists {
					ht.Remove(htKey, htVal)
					worker.ansOK()
				} else {
					worker.ansErr(R_ERR_SCHEMA, []byte{})
				}
			case C_QUERY_PRE:
				// (Increase pending-transaction counter)
				// Query operation is about to begin, increase pending-transaction counter.
				if worker.pendingTransaction {
					worker.ansErr(R_ERR, []byte("Client mishaved and asked for transaction twice"))
					worker.disconnect()
				} else {
					worker.pendingTransaction = true
					atomic.AddInt64(&worker.srv.pendingTransactions, 1)
					worker.ansOK()
				}
			case C_QUERY_POST:
				// (Decrease pending-transaction counter)
				// Query operation has completed, decrease pending-transaction counter.
				if worker.pendingTransaction {
					worker.pendingTransaction = false
					atomic.AddInt64(&worker.srv.pendingTransactions, -1)
					worker.ansOK()
				} else {
					worker.ansErr(R_ERR, []byte("Client mishaved and asked for ending transaction without starting one"))
					worker.disconnect()
				}
			case C_GO_MAINT:
				// Go to maintenance mode to allow exclusive data file access by the client
				if atomic.LoadInt64(&worker.srv.pendingTransactions) > 0 {
					// Do not allow maintenance access if another client is in the middle of document update
					worker.ansErr(R_ERR_MAINT, []byte(fmt.Sprintf("There are %d outstanding transactions", atomic.LoadInt64(&worker.srv.pendingTransactions))))
				} else {
					worker.pendingMaintenance = true
					worker.srv.maintByClient = worker.id
					worker.ansOK()
				}
			case C_RELOAD:
				// Reload my schema and increase schema revision number
				worker.srv.reload()
				worker.ansOK()
			case C_LEAVE_MAINT:
				// Leave maintenance mode, then reload my schema and increase schema revision number.
				worker.pendingMaintenance = false
				worker.srv.maintByClient = 0
				worker.srv.reload()
				worker.ansOK()
			case C_PING:
				// Respond OK with the client's ID, unless the server is in maintenance mode.
				worker.ansOK(Buint64(uint64(worker.srv.nProcs)), Buint64(worker.id))
			case C_SHUTDOWN:
				// Stop accepting new client connections, and inform existing clients to close their connection.
				worker.srv.Shutdown()
				worker.ansOK()
			default:
				worker.ansErr(R_ERR, []byte("Unknown command"))
				worker.disconnect()
				return
			}
		}
		worker.srv.opLock.Unlock()
	}
}
