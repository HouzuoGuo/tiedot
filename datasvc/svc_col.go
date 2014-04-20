// Data structure server - collection & document operations.
package datasvc

import (
	"errors"
	"fmt"
	"github.com/HouzuoGuo/tiedot/data"
)

// Open a collection file.
type ColOpenInput struct {
	Path, Name string
}

func (ds *DataSvc) ColOpen(in ColOpenInput, _ *bool) (err error) {
	ds.dataLock.Lock()
	defer ds.dataLock.Unlock()
	if _, alreadyOpened := ds.col[in.Name]; alreadyOpened {
		return errors.New("Collection is already opened")
	}
	ds.col[in.Name], err = data.OpenCollection(in.Path)
	return
}

// Synchronize a collection file.
func (ds *DataSvc) ColSync(name string, _ *bool) (err error) {
	ds.dataLock.Lock()
	defer ds.dataLock.Unlock()
	if col, exists := ds.col[name]; exists {
		err = col.Sync()
	} else {
		err = errors.New(fmt.Sprintf("Collection %s does not exist", name))
	}
	return
}

// Close a collection file.
func (ds *DataSvc) ColClose(name string, _ *bool) (err error) {
	ds.dataLock.Lock()
	defer ds.dataLock.Unlock()
	if col, exists := ds.col[name]; exists {
		err = col.Close()
	} else {
		err = errors.New(fmt.Sprintf("Collection %s does not exist", name))
	}
	delete(ds.col, name)
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
	if col, exists := ds.col[in.Name]; exists {
		*doc = string(col.Read(in.ID))
	} else {
		err = errors.New(fmt.Sprintf("Collection %s does not exist", in.Name))
	}
	return
}

// Insert a document.
type DocInsertInput struct {
	Name, Doc string
}

func (ds *DataSvc) DocInsert(in DocInsertInput, id *int) (err error) {
	ds.dataLock.Lock()
	defer ds.dataLock.Unlock()
	if col, exists := ds.col[in.Name]; exists {
		*id, err = col.Insert([]byte(in.Doc))
	} else {
		err = errors.New(fmt.Sprintf("Collection %s does not exist", in.Name))
	}
	return
}

// Update a document by ID.
type DocUpdateInput struct {
	Name, Doc string
	ID        int
}

func (ds *DataSvc) DocUpdate(in DocUpdateInput, newID *int) (err error) {
	ds.dataLock.Lock()
	defer ds.dataLock.Unlock()
	if col, exists := ds.col[in.Name]; exists {
		*newID, err = col.Update(in.ID, []byte(in.Doc))
	} else {
		err = errors.New(fmt.Sprintf("Collection %s does not exist", in.Name))
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
	if col, exists := ds.col[in.Name]; exists {
		err = col.Delete(in.ID)
	} else {
		err = errors.New(fmt.Sprintf("Collection %s does not exist", in.Name))
	}
	return
}
