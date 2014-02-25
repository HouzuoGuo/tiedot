/* Client connection to a tiedot IPC server rank. */
package network

import (
	"bufio"
	"github.com/HouzuoGuo/tiedot/tdlog"
	"net"
	"path"
	"strconv"
	"sync"
)

// A connection to tiedot RPC server
type Client struct {
	SrvAddr, IPCSrvTmpDir string
	SrvRank               int
	In                    *bufio.Reader
	Out                   *bufio.Writer
	Conn                  *net.Conn
	Mutex                 *sync.Mutex
}

// Create a connection to a tiedot IPC server.
func NewClient(ipcSrvTmpDir string, rank int) (tc *Client, err error) {
	addr := path.Join(ipcSrvTmpDir, strconv.Itoa(rank))
	conn, err := net.Dial("unix", addr)
	if err != nil {
		return
	}
	tc = &Client{SrvAddr: addr, IPCSrvTmpDir: ipcSrvTmpDir, SrvRank: rank,
		In: bufio.NewReader(conn), Out: bufio.NewWriter(conn), Conn: &conn,
		Mutex: new(sync.Mutex)}
	return
}

// Close the connection, shutdown client. Remember to call this!
func (tc *Client) ShutdownClient() {
	(*tc.Conn).Close()
	tdlog.Printf("Client has shutdown the connection to %s", tc.SrvAddr)
}
