/*
(Collection) Partition is a collection data file accompanied by a hash table in order to allow addressing of a
document using an unchanging ID:
The hash table stores the unchanging ID as entry key and the physical document location as entry value.
*/
package data

import (
	"errors"
	"github.com/HouzuoGuo/tiedot/tdlog"
)

// Partition associates a hash table with collection documents, allowing addressing of a document using an unchanging ID.
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

// Insert a document. The ID may be used to retrieve/update/delete the document later on.
func (part *Partition) Insert(id uint64, data []byte) (physID uint64, err error) {
	physID, err = part.col.Insert(data)
	if err != nil {
		return
	}
	part.lookup.Put(id, physID)
	return
}

// Find and retrieve a document by ID.
func (part *Partition) Read(id uint64) ([]byte, error) {
	physID := part.lookup.Get(id, 1)
	if len(physID) == 0 {
		return nil, errors.New("Document does not exist")
	} else if data := part.col.Read(physID[0]); data == nil {
		return nil, errors.New("Document does not exist")
	} else {
		return data, nil
	}
}

// Update a document.
func (part *Partition) Update(id uint64, data []byte) (err error) {
	physID := part.lookup.Get(id, 1)
	if len(physID) == 0 {
		return errors.New("Document does not exist")
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
		return errors.New("Document does not exist")
	}
	part.col.Delete(physID[0])
	part.lookup.Remove(id, physID[0])
	return
}

// Partition documents into roughly equally sized portions, and run the function on every document in the portion.
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

// Return approximate number of documents in the partition.
func (part *Partition) ApproxDocCount() uint64 {
	totalPart := uint64(24) // not magic; a larger number makes estimation less accurate, but improves performance
	for {
		keys, _ := part.lookup.GetPartition(0, totalPart)
		if len(keys) == 0 {
			if totalPart < 8 {
				return 0 // the hash table is really really empty
			}
			// Try a larger partition size
			totalPart = totalPart / 2
		} else {
			return uint64(float64(len(keys)) * float64(totalPart))
		}
	}
}

// Clear data file and lookup hash table.
func (part *Partition) Clear() (err error) {
	var failure bool
	if err = part.col.Clear(); err != nil {
		tdlog.CritNoRepeat("Failed to clear %s: %v", part.col.Path, err)
		failure = true
	}
	if err = part.lookup.Clear(); err != nil {
		tdlog.CritNoRepeat("Failed to clear %s: %v", part.lookup.Path, err)
		failure = true
	}
	if failure {
		err = errors.New("Operation did not complete successfully")
	}
	return
}

// Close file handles. Stop using the partition after the call!
func (part *Partition) Close() (err error) {
	var failure bool
	if err = part.col.Close(); err != nil {
		tdlog.CritNoRepeat("Failed to close %s: %v", part.col.Path, err)
		failure = true
	}
	if err = part.lookup.Close(); err != nil {
		tdlog.CritNoRepeat("Failed to close %s: %v", part.lookup.Path, err)
		failure = true
	}
	if failure {
		err = errors.New("Operation did not complete successfully")
	}
	return
}
