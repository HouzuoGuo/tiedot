// Database logic - data file/schema management.
package dbsvc

import (
	"fmt"
	"github.com/HouzuoGuo/tiedot/datasvc"
	"github.com/HouzuoGuo/tiedot/tdlog"
	"io/ioutil"
	"net/rpc"
	"path"
	"strconv"
	"strings"
	"time"
)

const (
	COL_NAME_SPLIT    = "_"
	IDX_PATH_SPLIT    = "!"
	LOOKUP_FILE_MAGIC = "id_"
	DAT_FILE_MAGIC    = "dat_"
	HT_DIR_MAGIC      = "ht_"
)

var discard *bool = new(bool)

// Lock down all data servers for exclusive access. Remember to call unlockAllData afterwards!
func (db *DBSvc) lockAllData() {
	for _, srv := range db.data {
		if err := srv.Call("DataSvc.Lock", true, discard); err != nil {
			panic(err)
		}
	}
}
func (db *DBSvc) unlockAllData() {
	for _, srv := range db.data {
		if err := srv.Call("DataSvc.Unlock", true, discard); err != nil {
			panic(err)
		}
	}
}

// Unload schema from all data servers
func (db *DBSvc) unloadAll() {
	for _, srv := range db.data {
		if err := srv.Call("DataSvc.Unload", true, discard); err != nil {
			panic(err)
		}
	}
}

// Lock a partition for exclusive access. Remember to call unlockPart afterwards!
func (db *DBSvc) lockPart(part *rpc.Client) {
	if err := part.Call("DataSvc.Lock", true, discard); err != nil {
		panic(err)
	}
}
func (db *DBSvc) unlockPart(part *rpc.Client) {
	if err := part.Call("DataSvc.Unlock", true, discard); err != nil {
		panic(err)
	}
}

// Make an RPC call to a data partition and deal with possible "schema version low" errors.
func (db *DBSvc) callPartition(part *rpc.Client, call string, in interface{}, out interface{}) error {
	db.lockPart(part)
	if err := part.Call(call, in, out); err != nil {
		if err.Error() == datasvc.SCHEMA_VERSION_LOW {
			if err2 := db.loadSchema(false); err != nil {
				db.unlockPart(part)
				return err2
			}
			db.unlockPart(part)
			return db.callPartition(part, call, in, out)
		}
		db.unlockPart(part)
		return err
	}
	db.unlockPart(part)
	return nil
}

// Reload my schema when server error indicates "schema version too low". Otherwise return the original error.
func (db *DBSvc) reactToSchemaUpdate(src error) error {
	if src.Error() == datasvc.SCHEMA_VERSION_LOW {
		if err := db.loadSchema(false); err == nil {
			return nil
		} else {
			return err
		}
	}
	return src
}

// Load DB schema into memory. Optionally load DB data files/index files into data servers.
func (db *DBSvc) loadSchema(loadIntoServers bool) error {
	dirContent, err := ioutil.ReadDir(db.dataDir)
	if err != nil {
		return err
	}
	db.schema = make(map[string]map[string][]string)
	for _, colDir := range dirContent {
		if !colDir.IsDir() {
			continue
		}
		if colName, numParts, err := db.destructColDirName(colDir.Name()); err != nil {
			tdlog.Printf("loadSchema: skipping %s", colDir.Name())
			continue
		} else {
			// Load the collection
			if numParts != db.totalRank {
				return fmt.Errorf("Number mismatch: there are %d servers, but collection %s has %d partitions", db.totalRank, colName, numParts)
			}
			db.schema[colName] = make(map[string][]string)
			// Open data partitions on data server
			if loadIntoServers {
				for i, srv := range db.data {
					if err := srv.Call("DataSvc.PartOpen", datasvc.PartOpenInput{
						path.Join(db.dataDir, colDir.Name(), DAT_FILE_MAGIC+strconv.Itoa(i)),
						path.Join(db.dataDir, colDir.Name(), LOOKUP_FILE_MAGIC+strconv.Itoa(i)),
						colName,
					}, discard); err != nil {
						panic(err)
					}
				}
			}
			// Load collection indexes
			dirContent, err := ioutil.ReadDir(path.Join(db.dataDir, colDir.Name()))
			if err != nil {
				return err
			}
			for _, htDir := range dirContent {
				if !(htDir.IsDir() && strings.HasPrefix(htDir.Name(), HT_DIR_MAGIC)) {
					continue
				}
				idxPath := strings.Split(htDir.Name()[len(HT_DIR_MAGIC):], IDX_PATH_SPLIT)
				if len(idxPath) < 2 {
					return fmt.Errorf("%s appears to be an index, however the dir name is malformed", htDir.Name())
				}
				idxPath = idxPath[1:]
				idxUID := mkIndexUID(colName, idxPath)
				db.schema[colName][idxUID] = idxPath
				// Open index partitions on data server
				if loadIntoServers {
					for i, srv := range db.data {
						if err := srv.Call("DataSvc.HTOpen", datasvc.HTOpenInput{
							path.Join(db.dataDir, colDir.Name(), htDir.Name(), strconv.Itoa(i)),
							idxUID,
						}, discard); err != nil {
							panic(err)
						}
					}
				}
			}
		}
	}
	db.mySchemaVersion = time.Now().UnixNano()
	tdlog.Printf("Schema reloaded: version number is now %d", db.mySchemaVersion)
	return nil
}

// Return total number of data server ranks (data partitions).
func (db *DBSvc) TotalRank() int {
	return db.totalRank
}
