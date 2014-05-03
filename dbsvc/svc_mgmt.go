// Database logic - schema management.
package dbsvc

import (
	"fmt"
	"github.com/HouzuoGuo/tiedot/datasvc"
	"github.com/HouzuoGuo/tiedot/tdlog"
	"io/ioutil"
	"path"
	"strconv"
	"strings"
	"time"
)

const (
	COL_NAME_SPLIT    = "_"
	IDX_ID_SPLIT      = "!"
	LOOKUP_FILE_MAGIC = "id_"
	DAT_FILE_MAGIC    = "dat_"
	HT_DIR_MAGIC      = "ht_"
)

var discard *bool = new(bool)

// Construct a unique ID for an index. The name may be used as the FS directory name for the index.
func mkIndexUID(colName string, idxPath []string) string {
	together := make([]string, len(idxPath)+1)
	together[0] = colName
	copy(together[1:], idxPath)
	return strings.Join(together, IDX_ID_SPLIT)
}

// Get collection name and indexed path from an index unique ID.
func destructIndexUID(indexUID string) (colName string, idxPath []string) {
	splitted := strings.Split(indexUID, IDX_ID_SPLIT)
	return splitted[0], splitted[1:]
}

// Load DB schema into memory. Optionally load DB data files/index files into data servers.
func (db *DBSvc) LoadSchema(loadIntoServers bool) error {
	dirContent, err := ioutil.ReadDir(db.dataDir)
	if err != nil {
		return err
	}
	db.schema = make(map[string]map[string][]string)
	for _, colDir := range dirContent {
		if !colDir.IsDir() {
			continue
		}
		// Collection directory name looks like: "WonderfulStuff_8"
		nameComps := strings.Split(colDir.Name(), COL_NAME_SPLIT)
		if len(nameComps) < 2 {
			continue
		} else if numParts, err := strconv.Atoi(nameComps[1]); err != nil {
			continue
		} else {
			// Load the collection
			colName := nameComps[0]
			if numParts != db.totalRank {
				return fmt.Errorf("Number mismatch: there are %d servers, but collection %s has %d partitions", db.totalRank, colName, numParts)
			}
			db.schema[colName] = make(map[string][]string)
			// Data partitions
			if loadIntoServers {
				for i, srv := range db.data {
					if err := srv.Call("DataSvc.PartOpen", datasvc.PartOpenInput{
						path.Join(db.dataDir, colDir.Name(), DAT_FILE_MAGIC+strconv.Itoa(i)),
						path.Join(db.dataDir, colDir.Name(), LOOKUP_FILE_MAGIC+strconv.Itoa(i)),
						colName,
					}, discard); err != nil {
						return err
					}
				}
			}
			// Indexes
			dirContent, err := ioutil.ReadDir(path.Join(db.dataDir, colDir.Name()))
			if err != nil {
				return err
			}
			for _, htDir := range dirContent {
				if !(htDir.IsDir() && strings.HasPrefix(htDir.Name(), HT_DIR_MAGIC)) {
					continue
				}
				idxPath := strings.Split(htDir.Name()[len(HT_DIR_MAGIC):], IDX_ID_SPLIT)
				idxUID := mkIndexUID(colName, idxPath)
				db.schema[colName][idxUID] = idxPath
				if loadIntoServers {
					for i, srv := range db.data {
						if err := srv.Call("DataSvc.HTOpen", datasvc.HTOpenInput{
							path.Join(db.dataDir, colDir.Name(), htDir.Name(), strconv.Itoa(i)),
							idxUID,
						}, discard); err != nil {
							return err
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
