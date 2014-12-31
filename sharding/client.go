// DB sharding via IPC using a binary protocol - client structure, connection logic, and message handling.
package sharding

import (
	"bufio"
	"fmt"
	"github.com/HouzuoGuo/tiedot/db"
	"github.com/HouzuoGuo/tiedot/tdlog"
	"math/rand"
	"net"
	"path"
	"strconv"
	"sync"
	"time"
)

// Client connects to ranks of sharded DB servers via Unix domain socket.
type RouterClient struct {
	workspace string
	id        uint64
	sock      []net.Conn
	in        []*bufio.Reader
	out       []*bufio.Writer
	nProcs    int
	opLock    *sync.Mutex
	schema    *Schema
}

// Create a client and immediately connect to all DB shard servers.
func NewClient(workspace string) (client *RouterClient, err error) {
	client = &RouterClient{
		id:        0,
		workspace: workspace,
		sock:      make([]net.Conn, 0, 8),
		in:        make([]*bufio.Reader, 0, 8),
		out:       make([]*bufio.Writer, 0, 8),
		opLock:    new(sync.Mutex),
		schema:    new(Schema)}
	// Connect to server 0
	for attempt := 0; attempt < 10; attempt++ {
		sockPath := path.Join(workspace, "0"+SOCK_FILE_SUFFIX)
		sock, err := net.Dial("unix", sockPath)
		if err != nil {
			time.Sleep(10 * time.Millisecond)
			continue
		}
		client.sock = append(client.sock, sock)
		client.in = append(client.in, bufio.NewReader(sock))
		client.out = append(client.out, bufio.NewWriter(sock))
		// Ask for my client ID and server nProcs
		if err = client.refreshClientInfo(); err != nil {
			return nil, err
		}
		break
	}
	if client.nProcs == 0 {
		return nil, fmt.Errorf("Client %d: failed to get number of server processes", client.id)
	}
	// Connect to remaining server processes
	for i := 1; i < client.nProcs; i++ {
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
			return nil, fmt.Errorf("Client %d: failed to connect to server no.%d of %d", client.id, i, client.nProcs)
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
				tdlog.Noticef("Client %d: lost connection with server(s), closing this client.", client.id)
				client.close()
				client.opLock.Unlock()
				return
			}
			client.opLock.Unlock()
			time.Sleep(1 * time.Second)
		}
	}()
	rand.Seed(time.Now().UnixNano())
	tdlog.Noticef("Client %d: successfully started and connected to %d server processes", client.id, client.nProcs)
	return
}

// Client sends a command and reads server's response.
func (client *RouterClient) sendCmd(rank int, retryOnSchemaRefresh bool, cmd byte, params ...[]byte) (retCode byte, moreInfo [][]byte, err error) {
	allParams := make([][]byte, len(params)+1)
	// Param 0 should be the client's schema revision
	allParams[0] = Buint32(client.schema.rev)
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
		tdlog.Noticef("Client %d: server shutdown has begun, closing this client.", client.id)
		client.close()
		err = fmt.Errorf("Server is shutting down")
	case R_ERR_SCHEMA:
		// Reload my schema on revision-mismatch
		client.reload(Uint32(moreInfo[0]))
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

// Reload client's schema.
func (client *RouterClient) reload(srvRev uint32) {
	clientDB, err := db.OpenDB(path.Join(client.workspace, "0"))
	if err != nil {
		panic(err)
	}
	client.schema.refreshToRev(clientDB, srvRev)
	if err = clientDB.Close(); err != nil {
		tdlog.Noticef("Client %d: failed to close database after a reload - %v", client.id, err)
	}
	tdlog.Infof("Client %d: schema reloaded to revision %d", client.id, srvRev)
	return
}

// Ping server to learn how many server processes there are and my client ID.
func (client *RouterClient) refreshClientInfo() error {
	for {
		retCode, info, err := client.sendCmd(0, true, C_PING)
		if retCode == R_ERR || retCode == R_ERR_DOWN {
			return err
		} else if retCode == R_ERR_MAINT {
			time.Sleep(time.Duration(100+rand.Intn(200)) * time.Millisecond)
			continue
		} else {
			client.nProcs = int(Uint64(info[0]))
			client.id = Uint64(info[1])
			return nil
		}
	}
}

// Reload schema on all server processes, and afterwards ping server to reload my schema.
func (client *RouterClient) reloadServer() error {
	for i := 0; i < client.nProcs; i++ {
		_, _, err := client.sendCmd(i, true, C_RELOAD)
		if err != nil {
			return err
		}
	}
	if err := client.ping(); err != nil {
		return err
	}
	return nil
}

func (client *RouterClient) reloadServerTest() error {
	client.opLock.Lock()
	defer client.opLock.Unlock()
	return client.reloadServer()
}

// Request maintenance access from all servers.
func (client *RouterClient) goMaint() (retCode byte, err error) {
	for goMaintSrv := range client.sock {
		if retCode, _, err = client.sendCmd(goMaintSrv, true, C_GO_MAINT); err != nil {
			for leaveMaintSrv := 0; leaveMaintSrv < goMaintSrv; leaveMaintSrv++ {
				retCode, _, err = client.sendCmd(leaveMaintSrv, true, C_LEAVE_MAINT)
				if err != nil {
					tdlog.Noticef("Client %d: failed to call LEAVE_MAINT on server %d - %v, closing this client.", client.id, leaveMaintSrv, err)
					client.close()
					return
				}
			}
			return
		}
	}
	return
}

// Request maintenance access from all servers, acquire client lock. Used only by test case!
func (client *RouterClient) goMaintTest() (retCode byte, err error) {
	client.opLock.Lock()
	defer client.opLock.Unlock()
	return client.goMaint()
}

// Remove maintenance access from all servers.
func (client *RouterClient) leaveMaint() error {
	for leaveMaintSrv := range client.sock {
		if _, _, err := client.sendCmd(leaveMaintSrv, true, C_LEAVE_MAINT); err != nil {
			return err
		}
	}
	return nil
}

// Request maintenance access from all servers, acquire client lock. Used only by test case!
func (client *RouterClient) leaveMaintTest() error {
	client.opLock.Lock()
	defer client.opLock.Unlock()
	return client.leaveMaint()
}

// Request maintenance access from servers, run the function, and finally remove maintenance access.
func (client *RouterClient) reqMaintAccess(fun func() error) error {
	client.opLock.Lock()
	defer client.opLock.Unlock()
	for {
		retCode, err := client.goMaint()
		switch retCode {
		case R_ERR_MAINT:
			tdlog.Infof("Client %d: servers are busy, will try again after a short delay - %v", client.id, err)
			time.Sleep(time.Duration(100+rand.Intn(200)) * time.Millisecond)
			continue
		case R_ERR_DOWN:
			fallthrough
		case CLIENT_IO_ERR:
			tdlog.Noticef("Client %d: IO error occured or servers are shutting down, this client is closed.", client.id)
			client.close()
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

func (client *RouterClient) ping() (err error) {
	for i := range client.sock {
		retCode, _, err := client.sendCmd(i, true, C_PING)
		if retCode != R_OK && retCode != R_ERR_MAINT {
			return fmt.Errorf("Ping error: server %d, code %d, err %v", i, retCode, err)
		}
	}
	return nil
}

// Ping all servers, and expect OK or ERR_MAINT response.
func (client *RouterClient) Ping() error {
	client.opLock.Lock()
	result := client.ping()
	client.opLock.Unlock()
	return result
}

func (client *RouterClient) close() {
	for _, sock := range client.sock {
		sock.Close()
	}
}

// Disconnect from all servers, and render the client useless.
func (client *RouterClient) Close() {
	client.opLock.Lock()
	defer client.opLock.Unlock()
	client.close()
	tdlog.Noticef("Client %d: closed on explicit request", client.id)
}

// Shutdown all servers and then close this client.
func (client *RouterClient) Shutdown() {
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
	client.close()
	tdlog.Noticef("Client %d: servers and this client are shut down on explicit request", client.id)
}
