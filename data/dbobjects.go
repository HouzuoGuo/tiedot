/*
DBObjects assigns a unique int32 ID to each collection and index.
Upon initialisation, the collection and indexes are loaded.
Upon reload, the "revision number" increases by one automatically, and the collections & indexes are re-loaded.
*/
package data

import (
	"fmt"
	"sort"
)

// Identify loaded collections and indexes using a unique integer ID.
type DBObjects struct {
	dbdir string
	rank  int
	rev   uint32

	colIDByName  map[string]int32
	htPathsByCol map[int32]map[int32][]string
	htIDByPath   map[int32]map[string]int32

	parts map[int32]*Partition
	hts   map[int32]*HashTable
}

// Initialise DBObjects without loading any objects (rank is set to -1).
func DBObjectsNew(dbdir string) (dbo *DBObjects) {
	dbo = &DBObjects{dbdir: dbdir, rank: -1, rev: 0,
		colIDByName:  make(map[string]int32),
		htPathsByCol: make(map[int32]map[int32][]string),
		htIDByPath:   make(map[int32]map[string]int32),
		parts:        make(map[int32]*Partition), hts: make(map[int32]*HashTable)}
	return
}

// Load collections and indexes.
func DBObjectsLoad(dbdir string, rank int) (dbo *DBObjects) {
	dbo = &DBObjects{dbdir: dbdir, rank: rank, rev: 0,
		colIDByName:  make(map[string]int32),
		htPathsByCol: make(map[int32]map[int32][]string),
		htIDByPath:   make(map[int32]map[string]int32),
		parts:        make(map[int32]*Partition), hts: make(map[int32]*HashTable)}
	dbo.Reload()
	return
}

// Return the current revision number.
func (dbo *DBObjects) GetCurrentRev() uint32 {
	return dbo.rev
}

// Look for a hash table's integer ID by collection name and index path segments. Return -1 if not found.
func (dbo *DBObjects) GetIndexIDBySplitPath(colID int32, indexPath []string) int32 {
	jointPath := JoinIndexPath(indexPath)
	htID, exists := dbo.htIDByPath[colID][jointPath]
	if !exists {
		return -1
	}
	return htID
}

// Re-read the database directory to gather the latest schema information.
func (dbo *DBObjects) Reload() {
	dbfs, err := DBReadDir(dbo.dbdir)
	if err != nil {
		panic(err)
	}

	dbo.colIDByName = make(map[string]int32)
	dbo.htPathsByCol = make(map[int32]map[int32][]string)
	dbo.htIDByPath = make(map[int32]map[string]int32)

	seq := int32(0)
	for _, colName := range dbfs.GetCollectionNamesSorted() {
		colID := seq

		dbo.colIDByName[colName] = colID
		dbo.htPathsByCol[colID] = make(map[int32][]string)
		dbo.htIDByPath[colID] = make(map[string]int32)

		seq++
		for _, jointPath := range dbfs.GetIndexesSorted(colName) {
			indexID := seq
			dbo.htIDByPath[colID][jointPath] = indexID
			dbo.htPathsByCol[colID][indexID] = SplitIndexPath(jointPath)
			seq++
		}
	}

	// Automatically increase schema revision by one
	dbo.rev++
	// Re-load partitions and hash tables.
	if dbo.rank == -1 {
		return
	}
	dbo.Close()
	dbo.parts = make(map[int32]*Partition)
	dbo.hts = make(map[int32]*HashTable)
	for colName, id := range dbo.colIDByName {
		colPath, idLookupPath := dbfs.GetCollectionDataFilePaths(colName, dbo.rank)
		part, err := OpenPartition(colPath, idLookupPath)
		if err != nil {
			panic(err)
		}
		dbo.parts[id] = part
	}
	for colName, colID := range dbo.colIDByName {
		for idxPath, idxID := range dbo.htIDByPath[colID] {
			ht, err := OpenHashTable(dbfs.GetIndexFilePath(colName, SplitIndexPath(idxPath), dbo.rank))
			if err != nil {
				panic(err)
			}
			dbo.hts[idxID] = ht
		}
	}
}

// Similar to Reload, but in addition it sets revision field to the specified value.
func (dbo *DBObjects) ReloadAndSetRev(newRev uint32) {
	dbo.Reload()
	dbo.rev = newRev
}

// Return an opened collection (document data partition) specified by the ID.
func (dbo *DBObjects) GetPartByID(id int32) (part *Partition, exists bool) {
	part, exists = dbo.parts[id]
	return
}

// Return an opened hash table specified by the ID.
func (dbo *DBObjects) GetHashTableByID(id int32) (ht *HashTable, exists bool) {
	ht, exists = dbo.hts[id]
	return
}

// Return collection ID by name.
func (dbo *DBObjects) GetColIDByName(name string) (id int32, exists bool) {
	id, exists = dbo.colIDByName[name]
	return
}

// Return all collection names, sorted alphabetically.
func (dbo *DBObjects) GetAllColNames() (names []string) {
	names = make([]string, 0, len(dbo.colIDByName))
	for name, _ := range dbo.colIDByName {
		names = append(names, name)
	}
	sort.Strings(names)
	return
}

// Return all indexes that belong to the collection, index paths are joint before return.
func (dbo *DBObjects) GetAllIndexPaths(colName string) (jointPaths []string, err error) {
	colID, found := dbo.colIDByName[colName]
	if !found {
		err = fmt.Errorf("Cannot find collection %s", colName)
		return
	}
	jointPaths = make([]string, 0, len(dbo.htIDByPath[colID]))
	for htPath, _ := range dbo.htIDByPath[colID] {
		jointPaths = append(jointPaths, htPath)
	}
	return
}

// Return index ID vs index paths for collection specified by ID.
func (dbo *DBObjects) GetIndexesJointPathByColID(colID int32) map[string]int32 {
	return dbo.htIDByPath[colID]
}

func (dbo *DBObjects) GetIndexesByColID(colID int32) map[int32][]string {
	return dbo.htPathsByCol[colID]
}

// Close opened collection files and indexes.
func (dbo *DBObjects) Close() {
	if dbo.rank == -1 {
		return
	}
	for _, ht := range dbo.hts {
		ht.Close()
	}
	for _, part := range dbo.parts {
		part.Close()
	}
}
