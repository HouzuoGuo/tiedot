// Database logic.
package dbsvc

import (
	"errors"
	"fmt"
	"github.com/HouzuoGuo/tiedot/tdlog"
	"net/rpc"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"
)

type DBSvc struct {
	srvWorkingDir   string
	dataDir         string
	totalRank       int
	data            []*rpc.Client                  // Connections to data partitions
	schema          map[string]map[string][]string // Collection => Index name => Index path ^^
	mySchemaVersion int64
	lock            *sync.Mutex
}

// Create a new Client, connect to all server ranks.
func NewDBSvc(totalRank int, srvWorkingDir string, dataDir string) (db *DBSvc, err error) {
	db = &DBSvc{srvWorkingDir, dataDir, totalRank,
		make([]*rpc.Client, totalRank), make(map[string]map[string][]string), time.Now().UnixNano(),
		new(sync.Mutex)}
	for i := 0; i < totalRank; i++ {
		if db.data[i], err = rpc.Dial("unix", path.Join(srvWorkingDir, strconv.Itoa(i))); err != nil {
			return
		}
	}
	return
}

// Shutdown all data partitions.
func (db *DBSvc) Shutdown() (err error) {
	db.lock.Lock()
	defer db.lock.Unlock()
	discard := new(bool)
	errs := make([]string, 0, 1)
	for i, srv := range db.data {
		if err := srv.Call("DataSvc.Shutdown", false, discard); err == nil || !strings.Contains(fmt.Sprint(err), "unexpected EOF") {
			errs = append(errs, fmt.Sprintf("Could not shutdown server rank %d", i))
		}
	}
	if len(errs) > 0 {
		err = errors.New(strings.Join(errs, "; "))
		tdlog.Errorf("Shutdown did not fully complete, but best effort has been made: %v", err)
	}
	return
}

// Construct a directory name for collection.
func (db *DBSvc) mkColDirName(colName string) string {
	return colName + COL_NAME_SPLIT + strconv.Itoa(db.totalRank)
}

// Get collection name and number of partitions from a collection directory name.
func (db *DBSvc) destructColDirName(dirName string) (string, int, error) {
	// Collection directory name looks like: "My_Wonderful_Stuff_8"
	split := strings.LastIndex(dirName, COL_NAME_SPLIT)
	if split == -1 {
		return "", 0, errors.New("Not a valid collection directory name")
	} else if split == 0 || split == len(dirName)-1 {
		return "", 0, errors.New("Not a valid collection directory name")
	} else if parts, err := strconv.Atoi(dirName[split+1:]); err != nil {
		return "", 0, errors.New("Not a valid collection directory name")
	} else {
		return dirName[0:split], parts, nil
	}
}

// Construct an index ID which uniquely identifies the index in a data partition.
func mkIndexUID(colName string, idxPath []string) string {
	together := make([]string, len(idxPath)+1)
	together[0] = colName
	copy(together[1:], idxPath)
	return strings.Join(together, IDX_PATH_SPLIT)
}

// Get collection name and indexed path from an index ID.
func destructIndexUID(indexUID string) (colName string, idxPath []string) {
	splitted := strings.Split(indexUID, IDX_PATH_SPLIT)
	return splitted[0], splitted[1:]
}
