// Database logic - collection management.
package dbsvc

import (
	"fmt"
	"github.com/HouzuoGuo/tiedot/datasvc"
	"io/ioutil"
	"path"
	"strconv"
	"strings"
)

const (
	SPLIT_MAGIC       = "_"
	LOOKUP_FILE_MAGIC = "id_"
	DAT_FILE_MAGIC    = "dat_"
	HT_DIR_MAGIC      = "ht_"
)

var discard *bool = new(bool)

func (db *DBSvc) loadCol(colPath, colName string, numParts int) error {
	if numParts != db.totalRank {
		return fmt.Errorf("Number mismatch: there are %d servers, but collection %s has %d partitions", db.totalRank, colName, numParts)
	}
	// Load data partitions
	for i, srv := range db.data {
		if err := srv.Call("DataSvc.PartOpen", datasvc.PartOpenInput{
			path.Join(db.dataDir, colPath, DAT_FILE_MAGIC+strconv.Itoa(i)),
			path.Join(db.dataDir, colPath, LOOKUP_FILE_MAGIC+strconv.Itoa(i)),
			colName,
		}, discard); err != nil {
			return err
		}
	}
	// Load indexes
	dirContent, err := ioutil.ReadDir(path.Join(db.dataDir, colPath))
	if err != nil {
		return err
	}
	for _, htDir := range dirContent {
		if !(htDir.IsDir() && strings.HasPrefix(htDir.Name(), HT_DIR_MAGIC)) {
			continue
		}
		htName := colName + SPLIT_MAGIC + htDir.Name()[len(HT_DIR_MAGIC):]
		for i, srv := range db.data {
			if err := srv.Call("DataSvc.HTOpen", datasvc.HTOpenInput{
				path.Join(db.dataDir, colPath, htDir.Name(), strconv.Itoa(i)),
				htName,
			}, discard); err != nil {
				return err
			}
		}
	}
	return nil
}

// Load DB data files/index files into data server ranks.
func (db *DBSvc) Load() error {
	dirContent, err := ioutil.ReadDir(db.dataDir)
	if err != nil {
		return err
	}
	for _, colDir := range dirContent {
		if !colDir.IsDir() {
			continue
		}
		// Collection directory name looks like: "WonderfulStuff_8"
		nameComps := strings.Split(colDir.Name(), SPLIT_MAGIC)
		if len(nameComps) < 2 {
			continue
		} else if numParts, err := strconv.Atoi(nameComps[1]); err != nil {
			continue
		} else if err := db.loadCol(colDir.Name(), nameComps[0], numParts); err != nil {
			return err
		}
	}
	return nil
}
