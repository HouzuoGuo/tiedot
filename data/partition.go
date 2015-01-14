/*
(Collection) Partition is a collection data file accompanied by a hash table in order to allow addressing of a
document using a persistent, unchanging ID irrespective of document growth and relocation.
The hash table stores ID in entry key and document physical location in entry value, both of which are uint64.
*/
package data

import (
	"github.com/HouzuoGuo/tiedot/dberr"
	"github.com/HouzuoGuo/tiedot/tdlog"
)

// Partition associates a hash table with collection documents, allowing addressing of a document using a persistent ID.
type Partition struct {
	col    *Collection
	lookup *HashTable
}

// Open a collection partition.
func OpenPartition(colPath, lookupPath string) (part *Partition, err error) {
	part = &Partition{}
	if part.col, err = OpenCollection(colPath); err != nil {
		return
	} else if part.lookup, err = OpenHashTable(lookupPath); err != nil {
		return
	}
	return
}

// Insert a new document and associate it with the specified ID. Return the physical location of inserted document.
func (part *Partition) Insert(id uint64, data []byte) (docLoc uint64, err error) {
	docLoc, err = part.col.Insert(data)
	if err != nil {
		return
	}
	part.lookup.Put(id, docLoc)
	return
}

// Retrieve a document by ID.
func (part *Partition) Read(id uint64) ([]byte, error) {
	physID := part.lookup.Get(id, 1)
	if len(physID) == 0 {
		return nil, dberr.New(dberr.ErrorNoDoc, id)
	}

	data := part.col.Read(physID[0])
	if data == nil {
		return nil, dberr.New(dberr.ErrorNoDoc, id)
	}
	return data, nil
}

// Update a document.
func (part *Partition) Update(id uint64, data []byte) (err error) {
	physID := part.lookup.Get(id, 1)
	if len(physID) == 0 {
		return dberr.New(dberr.ErrorNoDoc, id)
	}
	newID, err := part.col.Update(physID[0], data)
	if err != nil {
		return
	}
	if newID != physID[0] {
		part.lookup.Remove(id, physID[0])
		part.lookup.Put(id, newID)
	}
	return
}

// Delete a document.
func (part *Partition) Delete(id uint64) (err error) {
	physID := part.lookup.Get(id, 1)
	if len(physID) == 0 {
		return dberr.New(dberr.ErrorNoDoc, id)
	}
	part.col.Delete(physID[0])
	part.lookup.Remove(id, physID[0])
	return
}

// Partition documents into roughly equally sized portions, and run the function on each document in the portion.
func (part *Partition) ForEachDoc(partNum, totalPart uint64, fun func(id uint64, doc []byte) bool) (moveOn bool) {
	ids, physIDs := part.lookup.GetPartition(partNum, totalPart)
	for i, id := range ids {
		data := part.col.Read(physIDs[i])
		if data != nil {
			if !fun(id, data) {
				return false
			}
		}
	}
	return true
}

// Calculate approximate number of documents in the partition.
func (part *Partition) ApproxDocCount() uint64 {
	// Larger number = faster and less accurate; smaller number = slower and more accurate
	totalPart := uint64(18)
	for {
		keys, _ := part.lookup.GetPartition(0, totalPart)
		if len(keys) == 0 {
			if totalPart < 8 {
				return 0 // the hash table is looking really empty
			}
			// The hash table looks quite sparse, try a smaller partition size.
			totalPart = totalPart / 2
		} else {
			return uint64(float64(len(keys)) * float64(totalPart))
		}
	}
}

// Clear document data file and lookup hash table.
func (part *Partition) Clear() (err error) {
	if err = part.col.Clear(); err != nil {
		tdlog.CritNoRepeat("Failed to clear %s: %v", part.col.Path, err)
		err = dberr.New(dberr.ErrorIO)
	}
	if err = part.lookup.Clear(); err != nil {
		tdlog.CritNoRepeat("Failed to clear %s: %v", part.lookup.Path, err)
		err = dberr.New(dberr.ErrorIO)
	}
	return err
}

// Close file handles. Stop using the partition after the call!
func (part *Partition) Close() (err error) {
	if err = part.col.Close(); err != nil {
		tdlog.CritNoRepeat("Failed to close %s: %v", part.col.Path, err)
		err = dberr.New(dberr.ErrorIO)
	}
	if err = part.lookup.Close(); err != nil {
		tdlog.CritNoRepeat("Failed to close %s: %v", part.lookup.Path, err)
		err = dberr.New(dberr.ErrorIO)
	}
	return err
}
