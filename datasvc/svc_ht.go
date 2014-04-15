// Data structure server - hash table functions.
package datasvc

import (
	"errors"
	"github.com/HouzuoGuo/tiedot/data"
)

// Open a hash table file.
type OpenHTInput struct {
	Path, Name string
}

func (ds *DataSvc) OpenHT(in *OpenHTInput, _ *bool) (err error) {
	ds.dataLock.Lock()
	ds.schemaLock.Lock()
	if _, alreadyOpened := ds.ht[in.Name]; alreadyOpened {
		return errors.New("Hash table is already opened")
	}
	ds.ht[in.Name], err = data.OpenHashTable(in.Path)
	ds.schemaLock.Unlock()
	ds.dataLock.Unlock()
	return
}
