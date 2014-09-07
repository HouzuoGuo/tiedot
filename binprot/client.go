// Binary protocol over IPC - client.

package binprot

import (
	"bufio"
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
}

// Create a client and immediately connect to server.
func NewClient(workspace string) (client *BinProtClient, err error) {
	client = &BinProtClient{
		workspace: workspace,
		sock:      make([]net.Conn, 0, 8),
		in:        make([]*bufio.Reader, 0, 8),
		out:       make([]*bufio.Writer, 0, 8)}
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

// Ping server 0 and expect an OK response.
func (client *BinProtClient) Ping() (err error) {
	if err = ClientWriteCmd(client.out[0], C_PING); err != nil {
		return
	}
	_, _, err = ClientReadAns(client.in[0])
	return
}

// Put all servers into maintenance mode.
func (client *BinProtClient) GoMaint() (err error) {
	for i :=
}

// Disconnect from all servers, and render the client useless.
func (client *BinProtClient) Shutdown() {
	for _, sock := range client.sock {
		if err := sock.Close(); err != nil {
			tdlog.Noticef("Failed to close client socket: %v", err)
		}
	}
}
