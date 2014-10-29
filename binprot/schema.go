// Binary protocol over unix domain socket - schema models as seen by both server and client.

package binprot

import (
	"github.com/HouzuoGuo/tiedot/data"
	"github.com/HouzuoGuo/tiedot/db"
	"strings"
)

// Identify collections and indexes by an integer ID.
type Schema struct {
	colLookup     map[int32]*db.Col
	colNameLookup map[string]int32
	htLookup      map[int32]*data.HashTable
	indexPaths    map[int32]map[int32][]string
	rev           uint32
}

// To save bandwidth, both client and server refer collections and indexes by an int32 "ID", instead of using their string names.
func (schema *Schema) refresh(dbInstance *db.DB) {
	schema.colLookup = make(map[int32]*db.Col)
	schema.colNameLookup = make(map[string]int32)
	schema.htLookup = make(map[int32]*data.HashTable)
	schema.indexPaths = make(map[int32]map[int32][]string)

	// Both server and client run the same version of Go, therefore the order in which map keys are traversed is the same.
	seq := 0
	for _, colName := range dbInstance.AllCols() {
		col := dbInstance.Use(colName)
		colID := int32(seq)
		schema.colLookup[colID] = col
		schema.colNameLookup[colName] = colID
		schema.indexPaths[colID] = make(map[int32][]string)
		seq++
		for _, idxPath := range col.AllIndexes() {
			schema.htLookup[int32(seq)] = col.BPUseHT(strings.Join(idxPath, db.INDEX_PATH_SEP))
			schema.indexPaths[colID][int32(seq)] = idxPath
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
	for htID, htPath := range schema.indexPaths[schema.colNameLookup[colName]] {
		if strings.Join(idxPath, db.INDEX_PATH_SEP) == strings.Join(htPath, db.INDEX_PATH_SEP) {
			return htID
		}
	}
	return -1
}
