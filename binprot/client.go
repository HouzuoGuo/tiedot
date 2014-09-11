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
	"time"
)

// Bin protocol client connects to servers via Unix domain socket.
type BinProtClient struct {
	workspace string
	sock      []net.Conn
	in        []*bufio.Reader
	out       []*bufio.Writer
	rev       uint32
}

// Create a client and immediately connect to server.
func NewClient(workspace string) (client *BinProtClient, err error) {
	client = &BinProtClient{
		workspace: workspace,
		sock:      make([]net.Conn, 0, 8),
		in:        make([]*bufio.Reader, 0, 8),
		out:       make([]*bufio.Writer, 0, 8),
		rev:       1}
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
			client.in = append(client.in, bufio.NewReader(client.sock[i]))
			client.out = append(client.out, bufio.NewWriter(client.sock[i]))
			connSuccessful = true
			break
		}
		if !connSuccessful {
			if i == 0 {
				err = fmt.Errorf("No server seems to be running on %s", workspace)
			} else {
				tdlog.Noticef("Client successfully connected to %d server ranks", i)
			}
			return
		}
	}
	return
}

// Reload client's schema
func (client *BinProtClient) reload(srvRev uint32) {
	tdlog.Noticef("Reload client schema, to match server revision %d", srvRev)
	client.rev = srvRev
	return
}

// Client sends a "CMD-REV-PARAM-US-PARAM-RS" command to server.
func (client *BinProtClient) writeCmd(rank int, cmd byte, params ...[]byte) error {
	rev := make([]byte, 4)
	binary.LittleEndian.PutUint32(rev, client.rev)
	if err := client.out[rank].WriteByte(cmd); err != nil {
		return err
	} else if _, err := client.out[rank].Write(rev); err != nil {
		return err
	}
	for _, param := range params {
		if _, err := client.out[rank].Write(param); err != nil {
			return err
		} else if err := client.out[rank].WriteByte(C_US); err != nil {
			return err
		}
	}
	if err := client.out[rank].WriteByte(C_RS); err != nil {
		return err
	} else if err := client.out[rank].Flush(); err != nil {
		return err
	}
	return nil
}

// Client reads server's response.
func (client *BinProtClient) readAns(rank int) (moreInfo [][]byte, retCode byte, err error) {
	status, err := client.in[rank].ReadByte()
	if err != nil {
		return
	}
	reply, err := client.in[rank].ReadSlice(C_RS)
	if err != nil {
		return
	}
	moreInfo = bytes.Split(reply[:len(reply)-1], []byte{C_US})
	retCode = status
	//	fmt.Println("readAns", moreInfo, retCode, err)
	return
}

// Client sends a command and reads server's response.
func (client *BinProtClient) sendCmd(rank int, retryOnSchemaRefresh bool, cmd byte, params ...[]byte) (moreInfo [][]byte, retCode byte, err error) {
	for {
		if err = client.writeCmd(rank, cmd, params...); err != nil {
			return
		}
		moreInfo, retCode, err = client.readAns(rank)
		if retCode == C_ERR_SCHEMA {
			srvRev := moreInfo[0][0:4]
			client.reload(binary.LittleEndian.Uint32(srvRev))
			if retryOnSchemaRefresh {
				continue
			} else {
				err = fmt.Errorf("Server suggested schema mismatch")
				return
			}
		} else if retCode != C_OK {
			if len(moreInfo) > 0 && len(moreInfo[0]) > 0 {
				err = fmt.Errorf("Server returned error %d: %v", retCode, string(moreInfo[0]))
			} else {
				err = fmt.Errorf("Server returned error %d, no details available.", retCode)
			}
			return
		}
		return
	}
	return
}

// Ping server 0 and expect an OK response.
func (client *BinProtClient) Ping() error {
	for i := range client.sock {
		if _, _, err := client.sendCmd(i, true, C_PING); err != nil {
			return err
		}
	}
	return nil
}

// Request maintenance access from all servers.
func (client *BinProtClient) GoMaint() error {
	for goMaintSrv := range client.sock {
		if _, _, err := client.sendCmd(goMaintSrv, true, C_GO_MAINT); err != nil {
			for leaveMaintSrv := 0; leaveMaintSrv < goMaintSrv; leaveMaintSrv++ {
				if _, _, err := client.sendCmd(leaveMaintSrv, true, C_LEAVE_MAINT); err != nil {
					tdlog.Noticef("Failed to leaveMaint on server %d", leaveMaintSrv)
				}
			}
			return err
		}
	}
	return nil
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
	for {
		if err := client.GoMaint(); err == nil {
			break
		} else {
			tdlog.Noticef("Servers are busy, cannot shutdown yet - will retry soon.")
			time.Sleep(50 * time.Millisecond)
			continue
		}
	}
	for i := range client.sock {
		if _, _, err := client.sendCmd(i, true, C_SHUTDOWN); err != nil {
			tdlog.Noticef("Failed to shutdown server %d: %v", i, err)
		}
	}
	client.Close()
}

// Disconnect from all servers, and render the client useless.
func (client *BinProtClient) Close() {
	for _, sock := range client.sock {
		if err := sock.Close(); err != nil {
			tdlog.Noticef("Failed to close client socket: %v", err)
		}
	}
}
