// Data structure servers.
package datasvc

import (
	"github.com/HouzuoGuo/tiedot/data"
	"github.com/HouzuoGuo/tiedot/tdlog"
	"net"
	"net/rpc"
	"os"
	"path"
	"strconv"
	"sync"
)

// Data server is reponsible for some hash tables and collections, the server communicates via Unix domain socket.
type DataSvc struct {
	ht                   map[string]*data.HashTable
	col                  map[string]*data.Collection
	dataLock, schemaLock *sync.RWMutex
	rank                 int
	sockPath             string
}

// Create a new and blank data server.
func NewDataSvc(rank int) *DataSvc {
	return &DataSvc{ht: make(map[string]*data.HashTable), col: make(map[string]*data.Collection),
		dataLock: new(sync.RWMutex), schemaLock: new(sync.RWMutex),
		rank: rank, sockPath: path.Join(os.TempDir(), strconv.Itoa(rank))}
}

// Begin serving incoming connections. This function blocks until server is instructed by client to shutdown.
func (ds *DataSvc) Serve() (err error) {
	os.Remove(ds.sockPath)
	listener, err := net.Listen("unix", ds.sockPath)
	if err != nil {
		return
	}
	rpc.Register(ds)
	for {
		incoming, err := listener.Accept()
		if err != nil {
			tdlog.Errorf("Server %d: Client did not successfully establish connection: %v", ds.rank, err)
		}
		go rpc.ServeConn(incoming)
	}
	return
}
