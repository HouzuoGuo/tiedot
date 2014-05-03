// Data structure server - collection & document operations.
package datasvc

import (
	"errors"
	"fmt"
	"github.com/HouzuoGuo/tiedot/data"
	"time"
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
	if ds.part[in.Name], err = data.OpenPartition(in.ColPath, in.LookupPath); err != nil {
		ds.schemaVersion = time.Now().UnixNano()
	}
	return
}

// Synchronize a collection partition.
func (ds *DataSvc) PartSync(name string, _ *bool) error {
	ds.dataLock.Lock()
	defer ds.dataLock.Unlock()
	if part, exists := ds.part[name]; exists {
		return part.Sync()
	} else {
		return fmt.Errorf("Partition %s does not exist", name)
	}
}

// Close a collection partition.
func (ds *DataSvc) PartClose(name string, _ *bool) (err error) {
	ds.dataLock.Lock()
	defer ds.dataLock.Unlock()
	if part, exists := ds.part[name]; exists {
		err = part.Close()
		delete(ds.part, name)
		ds.schemaVersion = time.Now().UnixNano()
	} else {
		err = fmt.Errorf("Partition %s does not exist", name)
	}
	return
}

// Read a document by ID.
type DocReadInput struct {
	Name            string
	ID              int
	MySchemaVersion int64
}

func (ds *DataSvc) DocRead(in DocReadInput, doc *string) (err error) {
	ds.dataLock.RLock()
	defer ds.dataLock.RUnlock()
	if part, exists := ds.part[in.Name]; !exists {
		err = fmt.Errorf("Partition %s does not exist", in.Name)
	} else if in.MySchemaVersion < ds.schemaVersion {
		err = errors.New(SCHEMA_VERSION_LOW)
	} else {
		var docBytes []byte
		if docBytes, err = part.Read(in.ID); err == nil {
			*doc = string(docBytes)
		}
	}
	return
}

// Insert a document.
type DocInsertInput struct {
	Name, Doc       string
	ID              int
	MySchemaVersion int64
}

func (ds *DataSvc) DocInsert(in DocInsertInput, _ *bool) (err error) {
	ds.dataLock.Lock()
	defer ds.dataLock.Unlock()
	if part, exists := ds.part[in.Name]; !exists {
		err = fmt.Errorf("Partition %s does not exist", in.Name)
	} else if in.MySchemaVersion < ds.schemaVersion {
		err = errors.New(SCHEMA_VERSION_LOW)
	} else {
		_, err = part.Insert(in.ID, []byte(in.Doc))
	}
	return
}

// Update a document by ID.
type DocUpdateInput struct {
	Name, Doc       string
	ID              int
	MySchemaVersion int64
}

func (ds *DataSvc) DocUpdate(in DocUpdateInput, _ *bool) (err error) {
	ds.dataLock.Lock()
	defer ds.dataLock.Unlock()
	if part, exists := ds.part[in.Name]; !exists {
		err = fmt.Errorf("Partition %s does not exist", in.Name)
	} else if in.MySchemaVersion < ds.schemaVersion {
		err = errors.New(SCHEMA_VERSION_LOW)
	} else {
		err = part.Update(in.ID, []byte(in.Doc))
	}
	return
}

// Delete a document by ID.
type DocDeleteInput struct {
	Name            string
	ID              int
	MySchemaVersion int64
}

func (ds *DataSvc) DocDelete(in DocUpdateInput, _ *bool) (err error) {
	ds.dataLock.Lock()
	defer ds.dataLock.Unlock()
	if part, exists := ds.part[in.Name]; !exists {
		err = errors.New(fmt.Sprintf("Partition %s does not exist", in.Name))
	} else if in.MySchemaVersion < ds.schemaVersion {
		err = errors.New(SCHEMA_VERSION_LOW)
	} else {
		err = part.Delete(in.ID)
	}
	return
}

// Lock a document for exclusive update.
type DocLockUpdateInput struct {
	Name            string
	ID              int
	MySchemaVersion int64
}

func (ds *DataSvc) DocLockUpdate(in DocLockUpdateInput, _ *bool) (err error) {
	ds.dataLock.Lock()
	defer ds.dataLock.Unlock()
	if part, exists := ds.part[in.Name]; !exists {
		err = errors.New(fmt.Sprintf("Partition %s does not exist", in.Name))
	} else if in.MySchemaVersion < ds.schemaVersion {
		err = errors.New(SCHEMA_VERSION_LOW)
	} else {
		err = part.LockUpdate(in.ID)
	}
	return
}

// Unlock a document to make it ready for the next update.
type DocUnlockUpdateInput struct {
	Name            string
	ID              int
	MySchemaVersion int64
}

func (ds *DataSvc) DocUnlockUpdate(in DocUnlockUpdateInput, _ *bool) (err error) {
	ds.dataLock.Lock()
	defer ds.dataLock.Unlock()
	if part, exists := ds.part[in.Name]; !exists {
		err = errors.New(fmt.Sprintf("Partition %s does not exist", in.Name))
	} else if in.MySchemaVersion < ds.schemaVersion {
		err = errors.New(SCHEMA_VERSION_LOW)
	} else {
		part.UnlockUpdate(in.ID)
	}
	return
}
