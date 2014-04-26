// Data structure server - collection & document operations.
package datasvc

import (
	"errors"
	"fmt"
	"github.com/HouzuoGuo/tiedot/data"
)

// Open a collection partition.
type PartOpenInput struct {
	ColPath, LookupPath, Name string
}

func (ds *DataSvc) PartOpen(in PartOpenInput, _ *bool) (err error) {
	ds.dataLock.Lock()
	defer ds.dataLock.Unlock()
	if _, alreadyOpened := ds.part[in.Name]; alreadyOpened {
		return errors.New("Partition is already opened")
	}
	ds.part[in.Name], err = data.OpenPartition(in.ColPath, in.LookupPath)
	return
}

// Synchronize a collection partition.
func (ds *DataSvc) PartSync(name string, _ *bool) (err error) {
	ds.dataLock.Lock()
	defer ds.dataLock.Unlock()
	if part, exists := ds.part[name]; exists {
		err = part.Sync()
	} else {
		err = errors.New(fmt.Sprintf("Partition %s does not exist", name))
	}
	return
}

// Close a collection partition.
func (ds *DataSvc) PartClose(name string, _ *bool) (err error) {
	ds.dataLock.Lock()
	defer ds.dataLock.Unlock()
	if part, exists := ds.part[name]; exists {
		err = part.Close()
	} else {
		err = errors.New(fmt.Sprintf("Partition %s does not exist", name))
	}
	delete(ds.part, name)
	return
}

// Read a document by ID.
type DocReadInput struct {
	Name string
	ID   int
}

func (ds *DataSvc) DocRead(in DocReadInput, doc *string) (err error) {
	ds.dataLock.RLock()
	defer ds.dataLock.RUnlock()
	if part, exists := ds.part[in.Name]; exists {
		var docBytes []byte
		if docBytes, err = part.Read(in.ID); err == nil {
			*doc = string(docBytes)
		}
	} else {
		err = errors.New(fmt.Sprintf("Partition %s does not exist", in.Name))
	}
	return
}

// Insert a document.
type DocInsertInput struct {
	Name, Doc string
	ID        int
}

func (ds *DataSvc) DocInsert(in DocInsertInput, _ *bool) (err error) {
	ds.dataLock.Lock()
	defer ds.dataLock.Unlock()
	if part, exists := ds.part[in.Name]; exists {
		_, err = part.Insert(in.ID, []byte(in.Doc))
	} else {
		err = errors.New(fmt.Sprintf("Partition %s does not exist", in.Name))
	}
	return
}

// Update a document by ID.
type DocUpdateInput struct {
	Name, Doc string
	ID        int
}

func (ds *DataSvc) DocUpdate(in DocUpdateInput, _ *bool) (err error) {
	ds.dataLock.Lock()
	defer ds.dataLock.Unlock()
	if part, exists := ds.part[in.Name]; exists {
		err = part.Update(in.ID, []byte(in.Doc))
	} else {
		err = errors.New(fmt.Sprintf("Partition %s does not exist", in.Name))
	}
	return
}

// Delete a document by ID.
type DocDeleteInput struct {
	Name string
	ID   int
}

func (ds *DataSvc) DocDelete(in DocUpdateInput, _ *bool) (err error) {
	ds.dataLock.Lock()
	defer ds.dataLock.Unlock()
	if part, exists := ds.part[in.Name]; exists {
		err = part.Delete(in.ID)
	} else {
		err = errors.New(fmt.Sprintf("Partition %s does not exist", in.Name))
	}
	return
}
