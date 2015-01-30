/*
Schema opens a database directory and provides unique integer ID for each collection and index in database.
Schema revision is automatically increased by one on each reload.
*/
package db

import "github.com/HouzuoGuo/tiedot/data"

// Identify collections and indexes using a unique integer ID.
type Schema struct {
	rev uint32

	colIDByName map[string]int32

	htIDByPath map[int32]map[string]int32
	htPathByID map[int32]map[int32][]string
}

// Re-read the database directory to gather the latest schema information.
func (schema *Schema) Reload(dbfs *data.DBDirStruct) (err error) {
	if err != nil {
		return err
	}

	schema.colIDByName = make(map[string]int32)

	schema.htIDByPath = make(map[int32]map[int32][]string)
	schema.htPathByID = make(map[int32]map[string]int32)

	seq := int32(0)
	for _, colName := range dbfs.GetCollectionNamesSorted() {
		colID := seq

		schema.colIDByName[colName] = colID

		schema.htByID[colID] = make(map[int32][]string)
		schema.htIDByPath[colID] = make(map[string]int32)

		seq++
		for _, jointPath := range dbfs.GetIndexesSorted(colName) {
			indexID := seq
			splitIndexPath := data.SplitIndexPath(jointPath)

			schema.htIDByPath[colID][indexID] = splitIndexPath
			schema.htPathByID[colID][jointPath] = indexID
			seq++
		}
	}
	schema.rev++
	return nil
}

// Similar to Reload, but in addition it sets revision field to the specified value.
func (schema *Schema) ReloadAndSetRev(dbfs *data.DBDirStruct, newRev int32) (err error) {
	if err = schema.Reload(dbfs); err == nil {
		schema.rev = newRev
	}
	return
}

// Return the current revision number.
func (schema *Schema) GetCurrentRev() {
	return schema.rev
}

// Look for a hash table's integer ID by collection name and index path segments. Return -1 if not found.
func (schema *Schema) GetIndexIDBySplitPath(colName string, indexPath []string) int32 {
	jointPath := data.JoinIndexPath(indexPath)
	colID, exists := schema.colNameLookup[colName]
	if !exists {
		return -1
	}
	htID, exists := schema.htIDByPath[colID][jointPath]
	if !exists {
		return -1
	}
	return htID
}
