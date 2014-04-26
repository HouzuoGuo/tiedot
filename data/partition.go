// Collection partition.
package data

import (
	"errors"
	"github.com/HouzuoGuo/tiedot/tdlog"
)

// Collection partition consists of a document data file and a lookup table for locating document using unique IDs.
type Partition struct {
	col      *Collection
	lookup   *HashTable // For finding document physical location by ID
	updating map[int]struct{}
}

// Open a collection partition.
func OpenPartition(colPath, lookupPath string) (part *Partition, err error) {
	part = &Partition{updating: make(map[int]struct{})}
	if part.col, err = OpenCollection(colPath); err != nil {
		return
	} else if part.lookup, err = OpenHashTable(lookupPath); err != nil {
		return
	}
	return
}

// Insert a document.
func (part *Partition) Insert(id int, data []byte) (physID int, err error) {
	physID, err = part.col.Insert(data)
	if err != nil {
		return
	}
	part.lookup.Put(id, physID)
	return
}

// Read a document by ID.
func (part *Partition) Read(id int) ([]byte, error) {
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
func (part *Partition) Update(id int, data []byte) (err error) {
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

// Lock a document for exclusive update.
func (part *Partition) LockUpdate(id int) (err error) {
	if _, alreadyLocked := part.updating[id]; alreadyLocked {
		return errors.New("Document is already locked")
	}
	part.updating[id] = struct{}{}
	return
}

// Unlock a document to make it ready for the next update.
func (part *Partition) UnlockUpdate(id int) {
	delete(part.updating, id)
}

// Delete a document.
func (part *Partition) Delete(id int) (err error) {
	physID := part.lookup.Get(id, 1)
	if len(physID) == 0 {
		return errors.New("Document does not exist")
	}
	part.col.Delete(physID[0])
	part.lookup.Remove(id, physID[0])
	return
}

// Runt he function on every document.
func (part *Partition) ForEachDoc(fun func(id int, doc []byte) bool) {
	ids, physIDs := part.lookup.AllEntries(0)
	for i, id := range ids {
		data := part.col.Read(physIDs[i])
		if data != nil {
			if !fun(id, data) {
				return
			}
		}
	}
}

// Synchronize all file buffers.
func (part *Partition) Sync() (err error) {
	var failure bool
	if err = part.col.Sync(); err != nil {
		tdlog.Errorf("Failed to sync %s: %v", part.col.Path, err)
		failure = true
	}
	if err = part.lookup.Sync(); err != nil {
		tdlog.Errorf("Failed to sync %s: %v", part.lookup.Path, err)
		failure = true
	}
	if failure {
		err = errors.New("Operation did not complete successfully")
	}
	return
}

// Close all file handles.
func (part *Partition) Close() (err error) {
	var failure bool
	if err = part.col.Close(); err != nil {
		tdlog.Errorf("Failed to close %s: %v", part.col.Path, err)
		failure = true
	}
	if err = part.lookup.Close(); err != nil {
		tdlog.Errorf("Failed to close %s: %v", part.lookup.Path, err)
		failure = true
	}
	if failure {
		err = errors.New("Operation did not complete successfully")
	}
	return
}
