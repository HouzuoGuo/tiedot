// Binary protocol over IPC - client.

package binprot

import (
	"bufio"
	"fmt"
	"github.com/HouzuoGuo/tiedot/tdlog"
	"net"
	"path"
	"strconv"
)

// Bin protocol client connects to server via Unix domain socket.
type BinProtClient struct {
	rank                int
	workspace, sockPath string
	sock                net.Conn
	in                  *bufio.Reader
	out                 *bufio.Writer
}

// Create a client and immediately connect to server.
func NewClient(rank int, workspace string) (client *BinProtClient, err error) {
	client = &BinProtClient{
		rank:      rank,
		workspace: workspace,
		sockPath:  path.Join(workspace, strconv.Itoa(rank), SOCK_FILE)}
	if client.sock, err = net.Dial("unix", client.sockPath); err != nil {
		return
	}
	client.in = bufio.NewReader(client.sock)
	client.out = bufio.NewWriter(client.sock)
	return
}

// Ping server to test server reachability.
func (client *BinProtClient) Ping() (err error) {
	if err = ClientWriteCmd(client.out, C_PING); err != nil {
		return
	}
	_, err = ClientReadAns(client.in)
	return
}

// Ping server and expect an ERR response (for test case only).
func (client *BinProtClient) PingErr() (err error) {
	if err = ClientWriteCmd(client.out, C_PING_ERR); err != nil {
		return
	}
	if msg, err := ClientReadAns(client.in); err != nil || string(msg[0]) != "this is an error" {
		return fmt.Errorf("IO error or unexpected response: %v %v %v", msg[0], err, []byte("this is an error"))
	}
	return
}

// Disconnect from server, and render the client useless.
func (client *BinProtClient) Shutdown() {
	if err := client.sock.Close(); err != nil {
		tdlog.Noticef("Failed to close client socket: %v", err)
	}
}
