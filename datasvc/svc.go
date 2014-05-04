// Data structure servers.
package datasvc

import (
	"errors"
	"fmt"
	"github.com/HouzuoGuo/tiedot/data"
	"github.com/HouzuoGuo/tiedot/tdlog"
	"net"
	"net/rpc"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	SCHEMA_VERSION_LOW = "SCHEMA_VERSION_LOW" // Inform client that he should reload schema definition
)

// Data server is responsible for doing work on hash tables and collection partitions; the server communicates via Unix domain socket.
type DataSvc struct {
	ht                   map[string]*data.HashTable
	part                 map[string]*data.Partition
	schemaVersion        int64 // Unix timestamp in nanoseconds
	accessLock           *sync.RWMutex
	rank                 int
	workingDir, sockPath string
	listener             net.Listener
	clients              []net.Conn
	clientsLock          *sync.Mutex
}

// Create a new and blank data server.
func NewDataSvc(workingDir string, rank int) *DataSvc {
	return &DataSvc{ht: make(map[string]*data.HashTable), part: make(map[string]*data.Partition),
		accessLock:    new(sync.RWMutex),
		schemaVersion: time.Now().UnixNano(),
		rank:          rank, clients: make([]net.Conn, 0, 10), clientsLock: new(sync.Mutex),
		workingDir: workingDir, sockPath: path.Join(workingDir, strconv.Itoa(rank))}
}

// Begin serving incoming connections. This function blocks until server is instructed by client to shutdown.
func (ds *DataSvc) Serve() (err error) {
	os.MkdirAll(ds.workingDir, 0700)
	ds.listener, err = net.Listen("unix", ds.sockPath)
	if err != nil {
		return
	}
	rpcServer := rpc.NewServer()
	rpcServer.Register(ds)
	for {
		incoming, err := ds.listener.Accept()
		if err != nil {
			if strings.Contains(fmt.Sprint(err), "closed network connection") {
				break
			} else {
				tdlog.Errorf("Server %d: Client did not successfully establish connection: %v", ds.rank, err)
				continue
			}
		}
		ds.clientsLock.Lock()
		ds.clients = append(ds.clients, incoming)
		ds.clientsLock.Unlock()
		go rpcServer.ServeConn(incoming)
	}
	return
}

// Test the server RPC connection - return nil.
func (ds *DataSvc) Ping(_ bool, _ *bool) error {
	return nil
}

// Return server schema version number.
func (ds *DataSvc) SchemaVersion(_ bool, out *int64) error {
	*out = ds.schemaVersion
	return nil
}

// Lock the data server for exclusive access.
func (ds *DataSvc) Lock(_ bool, _ *bool) error {
	ds.accessLock.Lock()
	return nil
}

// Unlock from exclusive access.
func (ds *DataSvc) Unlock(_ bool, _ *bool) error {
	ds.accessLock.Unlock()
	return nil
}

// Shutdown server network routine and all client connections.
func (ds *DataSvc) Shutdown(_ bool, _ *bool) (err error) {
	errs := make([]string, 0, 1)
	if err = ds.listener.Close(); err != nil {
		errs = append(errs, fmt.Sprint(err))
	}
	for _, client := range ds.clients {
		if err = client.Close(); err != nil {
			errs = append(errs, fmt.Sprint(err))
		}
	}
	if len(errs) > 0 {
		err = errors.New(strings.Join(errs, "; "))
		tdlog.Errorf("Server %d: Shutdown did not fully complete, but best effort has been made: %v", ds.rank, err)
	}
	return
}
