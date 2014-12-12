// Binary protocol over unix domain socket - schema models as seen by both server and client.

package binprot

import (
	"github.com/HouzuoGuo/tiedot/data"
	"github.com/HouzuoGuo/tiedot/db"
	"strings"
)

// Identify collections and indexes by an integer ID.
type Schema struct {
	colLookup       map[int32]*db.Col
	colNameLookup   map[string]int32
	htLookup        map[int32]*data.HashTable
	indexPaths      map[int32]map[int32][]string
	indexPathsJoint map[int32]map[string]int32
	rev             uint32
}

// To save bandwidth, both client and server refer collections and indexes by an int32 "ID", instead of using their string names.
func (schema *Schema) refresh(dbInstance *db.DB) {
	schema.colLookup = make(map[int32]*db.Col)
	schema.colNameLookup = make(map[string]int32)
	schema.htLookup = make(map[int32]*data.HashTable)
	schema.indexPaths = make(map[int32]map[int32][]string)
	schema.indexPathsJoint = make(map[int32]map[string]int32)

	// Both server and client run the same version of Go, therefore the order in which map keys are traversed is the same.
	seq := 0
	for _, colName := range dbInstance.AllCols() {
		col := dbInstance.Use(colName)
		colID := int32(seq)
		schema.colLookup[colID] = col
		schema.colNameLookup[colName] = colID
		schema.indexPaths[colID] = make(map[int32][]string)
		schema.indexPathsJoint[colID] = make(map[int32]map[string]int32)
		seq++
		for _, idxPath := range col.AllIndexes() {
			jointPath := strings.Join(idxPath, db.INDEX_PATH_SEP)
			schema.htLookup[int32(seq)] = col.BPUseHT(jointPath)
			schema.indexPaths[colID][int32(seq)] = idxPath
			schema.indexPathsJoint[colID][jointPath] = int32(seq)
			seq++
		}
	}
	schema.rev++
}

func (schema *Schema) refreshToRev(dbInstance *db.DB, rev uint32) {
	schema.refresh(dbInstance)
	schema.rev = rev
}

// Look for a hash table's integer ID by collection name and index path segments.
func (schema *Schema) GetHTIDByPath(colName string, idxPath []string) int32 {
	jointPath := strings.Join(idxPath, db.INDEX_PATH_SEP)
	colID, exists := schema.colNameLookup[colName]
	if !exists {
		return -1
	}
	htID, exists := schema.indexPathsJoint[colID][jointPath]
	if !exists {
		return -1
	}
	return htID
}
