/* Client connection to a tiedot IPC server rank. */
package network

import (
	"bufio"
	"github.com/HouzuoGuo/tiedot/tdlog"
	"math/rand"
	"net"
	"path"
	"strconv"
	"sync"
	"time"
)

const (
	CONN_RETRY_INTERVAL = 1000 // milliseconds
	CONN_RETRY          = 10   // times
)

// A connection to tiedot RPC server
type Client struct {
	IPCSrvTmpDir string
	TotalRank    int
	In           []*bufio.Reader
	Out          []*bufio.Writer
	Conn         []net.Conn
	mutex        []*sync.Mutex
}

// Create a connection to a tiedot IPC server.
func NewClient(totalRank int, ipcSrvTmpDir string) (tc *Client, err error) {
	// It is very important for both client and server to initialize random seed
	rand.Seed(time.Now().UnixNano())
	tc = &Client{TotalRank: totalRank, IPCSrvTmpDir: ipcSrvTmpDir,
		In: make([]*bufio.Reader, totalRank), Out: make([]*bufio.Writer, totalRank),
		Conn: make([]net.Conn, totalRank), mutex: make([]*sync.Mutex, totalRank)}
	// Connect to all server ranks
	for i := 0; i < totalRank; i++ {
		addr := path.Join(ipcSrvTmpDir, strconv.Itoa(i))
		for retry := 0; retry < CONN_RETRY; retry++ {
			if tc.Conn[i], err = net.Dial("unix", addr); err == nil {
				break
			}
			time.Sleep(CONN_RETRY_INTERVAL * time.Millisecond)
		}
		if err != nil {
			return
		}
		tc.In[i] = bufio.NewReader(tc.Conn[i])
		tc.Out[i] = bufio.NewWriter(tc.Conn[i])
		tc.mutex[i] = new(sync.Mutex)
		tdlog.Printf("Client has connected with rank %d", i)
	}
	return
}

// Close the connection, shutdown client. Remember to call this!
func (tc *Client) ShutdownClient() {
	for _, mutex := range tc.mutex {
		mutex.Lock()
		defer mutex.Unlock()
	}
	for _, conn := range tc.Conn {
		conn.Close()
	}
	tdlog.Printf("Client has shutdown connections")
}
