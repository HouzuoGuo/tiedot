// Binary protocol over IPC - client.

package binprot

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/HouzuoGuo/tiedot/tdlog"
	"net"
	"path"
	"strconv"
	"sync"
	"time"
)

// Bin protocol client connects to servers via Unix domain socket.
type BinProtClient struct {
	workspace  string
	id         uint64
	sock       []net.Conn
	in         []*bufio.Reader
	out        []*bufio.Writer
	rev        uint32
	oneAtATime *sync.Mutex
	closeLock  *sync.Mutex
}

// Create a client and immediately connect to server.
func NewClient(workspace string) (client *BinProtClient, err error) {
	client = &BinProtClient{
		id:         0,
		workspace:  workspace,
		sock:       make([]net.Conn, 0, 8),
		in:         make([]*bufio.Reader, 0, 8),
		out:        make([]*bufio.Writer, 0, 8),
		rev:        1,
		oneAtATime: new(sync.Mutex),
		closeLock:  new(sync.Mutex)}
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
			client.closeLock.Lock()
			if client.Ping() != nil {
				tdlog.Noticef("Client %d: lost connection with servers", client.id)
				client.Close()
				client.closeLock.Unlock()
				return
			}
			client.closeLock.Unlock()
			time.Sleep(1 * time.Second)
		}
	}()
	tdlog.Noticef("Client %d: started", client.id)
	return
}

// Reload client's schema
func (client *BinProtClient) reload(srvRev uint32) {
	tdlog.Noticef("Client %d: reload schema to match server's schema revision %d", client.id, srvRev)
	client.rev = srvRev
	return
}

// Client sends a command and reads server's response.
func (client *BinProtClient) sendCmd(rank int, retryOnSchemaRefresh bool, cmd byte, params ...[]byte) (moreInfo [][]byte, retCode byte, err error) {
	client.oneAtATime.Lock()
	// Client sends a "CMD-REV-PARAM-US-PARAM-RS" command to server
	rev := make([]byte, 4)
	binary.LittleEndian.PutUint32(rev, client.rev)
	if err = client.out[rank].WriteByte(cmd); err != nil {
		client.oneAtATime.Unlock()
		return
	} else if _, err = client.out[rank].Write(rev); err != nil {
		client.oneAtATime.Unlock()
		return
	}
	for _, param := range params {
		if _, err = client.out[rank].Write(param); err != nil {
			client.oneAtATime.Unlock()
			return
		} else if err = client.out[rank].WriteByte(C_US); err != nil {
			client.oneAtATime.Unlock()
			return
		}
	}
	if err = client.out[rank].WriteByte(C_RS); err != nil {
		client.oneAtATime.Unlock()
		return
	} else if err = client.out[rank].Flush(); err != nil {
		client.oneAtATime.Unlock()
		return
	}
	// Client reads server's response
	statusByte, err := client.in[rank].ReadByte()
	if err != nil {
		client.oneAtATime.Unlock()
		return
	}
	reply, err := client.in[rank].ReadSlice(C_RS)
	if err != nil {
		client.oneAtATime.Unlock()
		return
	}
	moreInfo = bytes.Split(reply[:len(reply)-1], []byte{C_US})
	retCode = statusByte
	// Determine what to do with the return code
	switch retCode {
	case C_ERR_DOWN:
		// If server has already shut down, shut down client also
		client.Close()
		err = fmt.Errorf("Server is down")
	case C_ERR_SCHEMA:
		// May need to redo the command
		srvRev := moreInfo[0][0:4]
		client.reload(binary.LittleEndian.Uint32(srvRev))
		if retryOnSchemaRefresh {
			client.oneAtATime.Unlock()
			return client.sendCmd(rank, retryOnSchemaRefresh, cmd, params...)
		} else {
			err = fmt.Errorf("Server suggested schema mismatch")
		}
	default:
		if retCode != C_OK {
			if len(moreInfo) > 0 && len(moreInfo[0]) > 0 {
				err = fmt.Errorf("Server returned error %d: %v", retCode, string(moreInfo[0]))
			} else {
				err = fmt.Errorf("Server returned error %d, no details available.", retCode)
			}
		}
	}
	client.oneAtATime.Unlock()
	return
}

// Ping server 0 and expect an OK response.
func (client *BinProtClient) Ping() error {
	for i := range client.sock {
		myID, _, err := client.sendCmd(i, true, C_PING)
		if err != nil {
			return err
		}
		client.id = binary.LittleEndian.Uint64(myID[0])
	}
	return nil
}

// Request maintenance access from all servers.
func (client *BinProtClient) GoMaint() (retCode byte, err error) {
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
func (client *BinProtClient) LeaveMaint() error {
	for leaveMaintSrv := range client.sock {
		if _, _, err := client.sendCmd(leaveMaintSrv, true, C_LEAVE_MAINT); err != nil {
			return err
		}
	}
	return nil
}

// Shutdown all servers and then close this client.
func (client *BinProtClient) Shutdown() {
	client.closeLock.Lock()
	for {
		// Require maintenance access first
		retCode, err := client.GoMaint()
		switch retCode {
		case C_OK:
			// Proceed with server & client shutdown
			for i := range client.sock {
				if _, _, err := client.sendCmd(i, true, C_SHUTDOWN); err != nil {
					tdlog.Noticef("Client %d: failed to shutdown server %d - %v", client.id, i, err)
				}
			}
			client.Close()
			client.closeLock.Unlock()
			return
		case C_SHUTDOWN:
			// Proceed with client shutdown
			client.Close()
			client.closeLock.Unlock()
			return
		default:
			tdlog.Noticef("Client %d: servers are busy (%v), cannot shutdown yet - will retry soon.", err)
			time.Sleep(100 * time.Millisecond)
			continue
		}
	}
	client.closeLock.Unlock()
}

// Disconnect from all servers, and render the client useless.
func (client *BinProtClient) Close() {
	for _, sock := range client.sock {
		if err := sock.Close(); err != nil {
			tdlog.Noticef("Client %d: failed to close client socket: %v", client.id, err)
		}
	}
	tdlog.Noticef("Client %d: closed", client.id)
}
