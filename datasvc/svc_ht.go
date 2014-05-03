// Data structure server - hash table functions.
package datasvc

import (
	"errors"
	"fmt"
	"github.com/HouzuoGuo/tiedot/data"
	"time"
)

// Open a hash table file.
type HTOpenInput struct {
	Path, Name string
}

func (ds *DataSvc) HTOpen(in HTOpenInput, _ *bool) (err error) {
	ds.dataLock.Lock()
	defer ds.dataLock.Unlock()
	if _, alreadyOpened := ds.ht[in.Name]; alreadyOpened {
		return errors.New("Hash table is already opened")
	}
	if ds.ht[in.Name], err = data.OpenHashTable(in.Path); err != nil {
		ds.schemaVersion = time.Now().UnixNano()
	}
	return
}

// Synchronize a hash table file.
func (ds *DataSvc) HTSync(name string, _ *bool) (err error) {
	ds.dataLock.Lock()
	defer ds.dataLock.Unlock()
	if ht, exists := ds.ht[name]; exists {
		err = ht.Sync()
	} else {
		err = errors.New(fmt.Sprintf("Hash table %s does not exist", name))
	}
	return
}

// Close a hash table file.
func (ds *DataSvc) HTClose(name string, _ *bool) (err error) {
	ds.dataLock.Lock()
	defer ds.dataLock.Unlock()
	if ht, exists := ds.ht[name]; exists {
		err = ht.Close()
		ds.schemaVersion = time.Now().UnixNano()
	} else {
		err = errors.New(fmt.Sprintf("Hash table %s does not exist", name))
	}
	return
}

// Put an entry into hash table
type HTPutInput struct {
	Name            string
	Key, Val        int
	MySchemaVersion int64
}

func (ds *DataSvc) HTPut(in HTPutInput, _ *bool) (err error) {
	ds.dataLock.Lock()
	defer ds.dataLock.Unlock()
	if ht, exists := ds.ht[in.Name]; !exists {
		err = errors.New(fmt.Sprintf("Hash table %s does not exist", in.Name))
	} else if in.MySchemaVersion < ds.schemaVersion {
		err = errors.New(SCHEMA_VERSION_LOW)
	} else {
		ht.Put(in.Key, in.Val)
	}
	return
}

// Look up values by key.
type HTGetInput struct {
	Name            string
	Key, Limit      int
	MySchemaVersion int64
}

func (ds *DataSvc) HTGet(in HTGetInput, out *[]int) (err error) {
	ds.dataLock.Lock()
	defer ds.dataLock.Unlock()
	if ht, exists := ds.ht[in.Name]; !exists {
		err = errors.New(fmt.Sprintf("Hash table %s does not exist", in.Name))
	} else if in.MySchemaVersion < ds.schemaVersion {
		err = errors.New(SCHEMA_VERSION_LOW)
	} else {
		*out = ht.Get(in.Key, in.Limit)
	}
	return
}

// Flag a key-value pair as invalid.
type HTRemoveInput struct {
	Name            string
	Key, Val        int
	MySchemaVersion int64
}

func (ds *DataSvc) HTRemove(in HTRemoveInput, _ *bool) (err error) {
	ds.dataLock.Lock()
	defer ds.dataLock.Unlock()
	if ht, exists := ds.ht[in.Name]; !exists {
		err = errors.New(fmt.Sprintf("Hash table %s does not exist", in.Name))
	} else if in.MySchemaVersion < ds.schemaVersion {
		err = errors.New(SCHEMA_VERSION_LOW)
	} else {
		ht.Remove(in.Key, in.Val)
	}
	return
}

// Return all entries in hash table
type HTAllEntriesInput struct {
	Name            string
	Limit           int
	MySchemaVersion int64
}
type HTAllEntriesOutput struct {
	Keys, Vals []int
}

func (ds *DataSvc) HTAllEntries(in HTGetInput, out *HTAllEntriesOutput) (err error) {
	ds.dataLock.Lock()
	defer ds.dataLock.Unlock()
	if ht, exists := ds.ht[in.Name]; !exists {
		err = errors.New(fmt.Sprintf("Hash table %s does not exist", in.Name))
	} else if in.MySchemaVersion < ds.schemaVersion {
		err = errors.New(SCHEMA_VERSION_LOW)
	} else {
		keys, vals := ht.AllEntries(in.Limit)
		out.Keys = keys
		out.Vals = vals
	}
	return
}
