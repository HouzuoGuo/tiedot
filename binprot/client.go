// Binary protocol over IPC - client messaging.

package binprot

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/HouzuoGuo/tiedot/data"
	"github.com/HouzuoGuo/tiedot/db"
	"github.com/HouzuoGuo/tiedot/tdlog"
	"net"
	"path"
	"strconv"
	"sync"
	"time"
)

const C_ERR_IO = 255

// Bin protocol client connects to servers via Unix domain socket.
type BinProtClient struct {
	workspace     string
	id            uint64
	sock          []net.Conn
	in            []*bufio.Reader
	out           []*bufio.Writer
	rev           uint32
	opLock        *sync.Mutex
	colLookup     map[int32]*db.Col
	colNameLookup map[string]int32
	htLookup      map[int32]*data.HashTable
	htNameLookup  map[string]map[string]int32
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
		opLock:    new(sync.Mutex),
		colLookup: make(map[int32]*db.Col),
		htLookup:  make(map[int32]*data.HashTable)}
	client.reload(0)
	// Connect to servers, one at a time.
	for i := 0; ; i++ {
		connSuccessful := false
		for attempt := 0; attempt < 5; attempt++ {
			sockPath := path.Join(workspace, strconv.Itoa(i), SOCK_FILE)
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
				tdlog.Noticef("Client %d: successfully connected to %d server processes", client.id, i)
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
	tdlog.Noticef("Client %d: started", client.id)
	return
}

// Client sends a command and reads server's response.
func (client *BinProtClient) sendCmd(rank int, retryOnSchemaRefresh bool, cmd byte, params ...[]byte) (moreInfo [][]byte, retCode byte, err error) {
	// Client sends a "CMD-REV-PARAM-US-PARAM-RS" command to server
	rev := make([]byte, 4)
	binary.LittleEndian.PutUint32(rev, client.rev)
	if err = client.out[rank].WriteByte(cmd); err != nil {
		retCode = C_ERR_IO
		return
	} else if _, err = client.out[rank].Write(rev); err != nil {
		retCode = C_ERR_IO
		return
	}
	for _, param := range params {
		if _, err = client.out[rank].Write(param); err != nil {
			retCode = C_ERR_IO
			return
		} else if err = client.out[rank].WriteByte(C_US); err != nil {
			retCode = C_ERR_IO
			return
		}
	}
	if err = client.out[rank].WriteByte(C_RS); err != nil {
		retCode = C_ERR_IO
		return
	} else if err = client.out[rank].Flush(); err != nil {
		retCode = C_ERR_IO
		return
	}
	// Client reads server's response
	statusByte, err := client.in[rank].ReadByte()
	if err != nil {
		retCode = C_ERR_IO
		return
	}
	reply, err := client.in[rank].ReadSlice(C_RS)
	if err != nil {
		retCode = C_ERR_IO
		return
	}
	moreInfo = bytes.Split(reply[:len(reply)-1], []byte{C_US})
	retCode = statusByte
	// Determine what to do with the return code
	switch retCode {
	case C_OK:
		// Request-response all OK
	case C_ERR_DOWN:
		// If server has already shut down, shut down client also
		for _, sock := range client.sock {
			sock.Close()
		}
		tdlog.Noticef("Client %d: server shutdown has begun and this client is closed", client.id)
		err = fmt.Errorf("Server is shutting down")
	case C_ERR_SCHEMA:
		// Always reload my schema
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
	// Support numeric lookup of collections and hash tables
	client.colLookup, client.colNameLookup, client.htLookup, client.htNameLookup = mkSchemaLookupTables(clientDB)
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
		if _, retCode, err = client.sendCmd(goMaintSrv, true, C_GO_MAINT); err != nil {
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

// Remove maintenance access from all servers.
func (client *BinProtClient) leaveMaint() error {
	for leaveMaintSrv := range client.sock {
		if _, _, err := client.sendCmd(leaveMaintSrv, true, C_LEAVE_MAINT); err != nil {
			return err
		}
	}
	return nil
}

// Request maintenance access from servers, run the function, and finally remove maintenance access.
func (client *BinProtClient) reqMaintAccess(fun func() error) error {
	client.opLock.Lock()
	defer client.opLock.Unlock()
	for {
		retCode, err := client.goMaint()
		switch retCode {
		case C_ERR_MAINT:
			tdlog.Noticef("Client %d: servers are busy, will try again after a short delay - %v", client.id, err)
			time.Sleep(100 * time.Millisecond)
			continue
		case C_ERR_DOWN:
			fallthrough
		case C_ERR_IO:
			for _, sock := range client.sock {
				sock.Close()
			}
			tdlog.Noticef("Client %d: IO error occured or servers are shutting down, this client is closed.", client.id)
			return fmt.Errorf("Servers are down before maintenance operation can take place - %v", err)
		case C_OK:
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
		myID, retCode, err := client.sendCmd(i, true, C_PING)
		switch retCode {
		case C_OK:
			// Server returns my client ID
			// The client ID will not change in the next Ping call
			client.id = binary.LittleEndian.Uint64(myID[0])
		case C_ERR_MAINT:
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
