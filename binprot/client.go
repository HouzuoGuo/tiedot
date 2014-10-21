// Binary protocol over IPC - client messaging.

package binprot

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"github.com/HouzuoGuo/tiedot/data"
	"github.com/HouzuoGuo/tiedot/db"
	"github.com/HouzuoGuo/tiedot/tdlog"
	"math/rand"
	"net"
	"path"
	"strconv"
	"sync"
	"time"
)

// Bin protocol client connects to servers via Unix domain socket.
type BinProtClient struct {
	workspace     string
	id            uint64
	sock          []net.Conn
	in            []*bufio.Reader
	out           []*bufio.Writer
	nProcs        int
	rev           uint32
	opLock        *sync.Mutex
	colLookup     map[int32]*db.Col
	colNameLookup map[string]int32
	htLookup      map[int32]*data.HashTable
	indexPaths    map[int32]map[int32][]string
}

// Create a client and immediately connect to server.
func NewClient(workspace string) (client *BinProtClient, err error) {
	client = &BinProtClient{
		id:        0,
		workspace: workspace,
		sock:      make([]net.Conn, 0, 8),
		in:        make([]*bufio.Reader, 0, 8),
		out:       make([]*bufio.Writer, 0, 8),
		rev:       0,
		opLock:    new(sync.Mutex)}
	client.reload(0)
	// Connect to servers, one at a time.
	for i := 0; ; i++ {
		connSuccessful := false
		for attempt := 0; attempt < 5; attempt++ {
			sockPath := path.Join(workspace, strconv.Itoa(i)+SOCK_FILE_SUFFIX)
			sock, err := net.Dial("unix", sockPath)
			if err != nil {
				time.Sleep(50 * time.Millisecond)
				continue
			}
			client.sock = append(client.sock, sock)
			client.in = append(client.in, bufio.NewReader(sock))
			client.out = append(client.out, bufio.NewWriter(sock))
			connSuccessful = true
			break
		}
		if !connSuccessful {
			if i == 0 {
				err = fmt.Errorf("No server seems to be running on %s", workspace)
			} else {
				// First test
				if err = client.Ping(); err != nil {
					return
				}
				client.nProcs = i
				tdlog.Noticef("Client %d: successfully connected to %d server processes", client.id, client.nProcs)
			}
			break
		}
	}
	/*
		Server does not track connected clients in a central structure. Sending shutdown command to server merely sets
		a state flag and stops it from accepting new connections; existing workers (one per each client) remain running.
		Having the worker goroutines running prevents server process from exiting, and therefore every client pings all
		servers at regular interval.
	*/
	go func() {
		for {
			client.opLock.Lock()
			if err := client.ping(); err != nil {
				for _, sock := range client.sock {
					sock.Close()
				}
				tdlog.Noticef("Client %d: lost connection with server(s) and this client is closed", client.id)
				client.opLock.Unlock()
				return
			}
			client.opLock.Unlock()
			time.Sleep(1 * time.Second)
		}
	}()
	rand.Seed(time.Now().UnixNano())
	tdlog.Noticef("Client %d: started", client.id)
	return
}

// Client sends a command and reads server's response.
func (client *BinProtClient) sendCmd(rank int, retryOnSchemaRefresh bool, cmd byte, params ...[]byte) (retCode byte, moreInfo [][]byte, err error) {
	allParams := make([][]byte, len(params)+1)
	// Param 0 should be the client's schema revision
	rev := make([]byte, 4)
	binary.LittleEndian.PutUint32(rev, client.rev)
	allParams[0] = rev
	// Copy down the remaining params
	for i, param := range params {
		allParams[i+1] = param
	}
	// Client sends command to server
	if err = writeRec(client.out[rank], cmd, allParams...); err != nil {
		retCode = CLIENT_IO_ERR
		return
	}
	// Client reads server response
	retCode, moreInfo, err = readRec(client.in[rank])
	if err != nil {
		retCode = CLIENT_IO_ERR
		return
	}
	// Determine what to do with the return code
	switch retCode {
	case R_OK:
		// Request-response all OK
	case R_ERR_DOWN:
		// If server has been instructed to shut down, shut down client also.
		for _, sock := range client.sock {
			sock.Close()
		}
		tdlog.Noticef("Client %d: server shutdown has begun and this client is closed", client.id)
		err = fmt.Errorf("Server is shutting down")
	case R_ERR_SCHEMA:
		// Reload my schema on reivison-mismatch
		srvRev := moreInfo[0][0:4]
		client.reload(binary.LittleEndian.Uint32(srvRev))
		// May need to redo the command
		if retryOnSchemaRefresh {
			return client.sendCmd(rank, retryOnSchemaRefresh, cmd, params...)
		} else {
			err = fmt.Errorf("Server suggested schema mismatch")
		}
	default:
		if len(moreInfo) > 0 && len(moreInfo[0]) > 0 {
			err = fmt.Errorf("Server returned error %d: %v", retCode, string(moreInfo[0]))
		} else {
			err = fmt.Errorf("Server returned error %d, no more details available.", retCode)
		}
	}
	return
}

// Reload client's schema
func (client *BinProtClient) reload(srvRev uint32) {
	clientDB, err := db.OpenDB(path.Join(client.workspace, "0"))
	if err != nil {
		panic(err)
	}
	client.colLookup, client.colNameLookup, client.htLookup, client.indexPaths = mkSchemaLookupTables(clientDB)
	if err = clientDB.Close(); err != nil {
		tdlog.Noticef("Client %d: failed to close database after a reload - %v", client.id, err)
	}
	tdlog.Noticef("Client %d: schema has been reloaded to match server's schema revision %d", client.id, srvRev)
	client.rev = srvRev
	return
}

// Request maintenance access from all servers.
func (client *BinProtClient) goMaint() (retCode byte, err error) {
	for goMaintSrv := range client.sock {
		if retCode, _, err = client.sendCmd(goMaintSrv, true, C_GO_MAINT); err != nil {
			for leaveMaintSrv := 0; leaveMaintSrv < goMaintSrv; leaveMaintSrv++ {
				if _, _, err := client.sendCmd(leaveMaintSrv, true, C_LEAVE_MAINT); err != nil {
					tdlog.Noticef("Client %d: failed to call LEAVE_MAINT on server %d", client.id, leaveMaintSrv)
				}
			}
			return
		}
	}
	return
}

// Request maintenance access from all servers, acquire client lock. Used only by test case!
func (client *BinProtClient) goMaintTest() (retCode byte, err error) {
	client.opLock.Lock()
	defer client.opLock.Unlock()
	return client.goMaint()
}

// Remove maintenance access from all servers.
func (client *BinProtClient) leaveMaint() error {
	for leaveMaintSrv := range client.sock {
		if _, _, err := client.sendCmd(leaveMaintSrv, true, C_LEAVE_MAINT); err != nil {
			return err
		}
	}
	return nil
}

// Request maintenance access from all servers, acquire client lock. Used only by test case!
func (client *BinProtClient) leaveMaintTest() error {
	client.opLock.Lock()
	defer client.opLock.Unlock()
	return client.leaveMaint()
}

// Request maintenance access from servers, run the function, and finally remove maintenance access.
func (client *BinProtClient) reqMaintAccess(fun func() error) error {
	client.opLock.Lock()
	defer client.opLock.Unlock()
	for {
		retCode, err := client.goMaint()
		switch retCode {
		case R_ERR_MAINT:
			tdlog.Noticef("Client %d: servers are busy, will try again after a short delay - %v", client.id, err)
			time.Sleep(time.Duration(50+rand.Intn(100)) * time.Millisecond)
			continue
		case R_ERR_DOWN:
			fallthrough
		case CLIENT_IO_ERR:
			for _, sock := range client.sock {
				sock.Close()
			}
			tdlog.Noticef("Client %d: IO error occured or servers are shutting down, this client is closed.", client.id)
			return fmt.Errorf("Servers are down before maintenance operation can take place - %v", err)
		case R_OK:
			funResult := fun()
			if err := client.leaveMaint(); err != nil {
				return fmt.Errorf("Function error: %v, client LEAVE_MAINT error: %v", funResult, err)
			}
			return funResult
		}
	}
}

func (client *BinProtClient) ping() error {
	for i := range client.sock {
		retCode, myID, err := client.sendCmd(i, true, C_PING)
		switch retCode {
		case R_OK:
			// Server returns my client ID
			// The client ID will not change in the next Ping call
			client.id = binary.LittleEndian.Uint64(myID[0])
		case R_ERR_MAINT:
			// Server does not return my client ID, but server is alive.
		default:
			return fmt.Errorf("Ping error: code %d, err %v", retCode, err)
		}
	}
	return nil
}

// Ping all servers, and expect OK or ERR_MAINT response.
func (client *BinProtClient) Ping() error {
	client.opLock.Lock()
	result := client.ping()
	client.opLock.Unlock()
	return result
}

// Disconnect from all servers, and render the client useless.
func (client *BinProtClient) Close() {
	client.opLock.Lock()
	defer client.opLock.Unlock()
	for _, sock := range client.sock {
		sock.Close()
	}
	tdlog.Noticef("Client %d: closed on request", client.id)
}

// Shutdown all servers and then close this client.
func (client *BinProtClient) Shutdown() {
	client.reqMaintAccess(func() error {
		for i := range client.sock {
			if _, _, err := client.sendCmd(i, true, C_SHUTDOWN); err != nil {
				tdlog.Noticef("Client %d: failed to shutdown server %d - %v", client.id, i, err)
			}
		}
		return nil
	})
	client.opLock.Lock()
	defer client.opLock.Unlock()
	for _, sock := range client.sock {
		sock.Close()
	}
	tdlog.Noticef("Client %d: servers have been asked to shutdown, this client is closed.", client.id)
}
