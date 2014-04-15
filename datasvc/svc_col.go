// Data structure server - collection & document operations.
package datasvc

import (
	"errors"
	"github.com/HouzuoGuo/tiedot/data"
)

// Open a collection file.
type OpenColInput struct {
	Path, Name string
}

func (ds *DataSvc) OpenCol(in *OpenColInput, _ *bool) (err error) {
	ds.dataLock.Lock()
	ds.schemaLock.Lock()
	if _, alreadyOpened := ds.col[in.Name]; alreadyOpened {
		return errors.New("Collection is already opened")
	}
	ds.col[in.Name], err = data.OpenCollection(in.Path)
	ds.schemaLock.Unlock()
	ds.dataLock.Unlock()
	return
}
