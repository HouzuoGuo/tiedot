// Data structure server - hash table functions.
package datasvc

import (
	"errors"
	"fmt"
	"github.com/HouzuoGuo/tiedot/data"
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
	ds.ht[in.Name], err = data.OpenHashTable(in.Path)
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
	} else {
		err = errors.New(fmt.Sprintf("Hash table %s does not exist", name))
	}
	return
}

// Put an entry into hash table
type HTPutInput struct {
	Name     string
	Key, Val int
}

func (ds *DataSvc) HTPut(in HTPutInput, _ *bool) (err error) {
	ds.dataLock.Lock()
	defer ds.dataLock.Unlock()
	if ht, exists := ds.ht[in.Name]; exists {
		ht.Put(in.Key, in.Val)
	} else {
		err = errors.New(fmt.Sprintf("Hash table %s does not exist", in.Name))
	}
	return
}

// Look up values by key.
type HTGetInput struct {
	Name       string
	Key, Limit int
}

func (ds *DataSvc) HTGet(in HTGetInput, out *[]int) (err error) {
	ds.dataLock.Lock()
	defer ds.dataLock.Unlock()
	if ht, exists := ds.ht[in.Name]; exists {
		*out = ht.Get(in.Key, in.Limit)
	} else {
		err = errors.New(fmt.Sprintf("Hash table %s does not exist", in.Name))
	}
	return
}

// Flag a key-value pair as invalid.
type HTRemoveInput struct {
	Name     string
	Key, Val int
}

func (ds *DataSvc) HTRemove(in HTRemoveInput, _ *bool) (err error) {
	ds.dataLock.Lock()
	defer ds.dataLock.Unlock()
	if ht, exists := ds.ht[in.Name]; exists {
		ht.Remove(in.Key, in.Val)
	} else {
		err = errors.New(fmt.Sprintf("Hash table %s does not exist", in.Name))
	}
	return
}

// Return all entries in hash table
type HTAllEntriesInput struct {
	Name  string
	Limit int
}
type HTAllEntriesOutput struct {
	Keys, Vals []int
}

func (ds *DataSvc) HTAllEntries(in HTGetInput, out *HTAllEntriesOutput) (err error) {
	ds.dataLock.Lock()
	defer ds.dataLock.Unlock()
	if ht, exists := ds.ht[in.Name]; exists {
		keys, vals := ht.AllEntries(in.Limit)
		out.Keys = keys
		out.Vals = vals
	} else {
		err = errors.New(fmt.Sprintf("Hash table %s does not exist", in.Name))
	}
	return
}
