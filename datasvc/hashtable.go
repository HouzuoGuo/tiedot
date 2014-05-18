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
	if _, alreadyOpened := ds.ht[in.Name]; alreadyOpened {
		return errors.New("Hash table is already opened")
	}
	if ds.ht[in.Name], err = data.OpenHashTable(in.Path); err == nil {
		ds.schemaVersion = time.Now().UnixNano()
	}
	return
}

// Synchronize a hash table file.
func (ds *DataSvc) HTSync(name string, _ *bool) (err error) {
	if ht, exists := ds.ht[name]; exists {
		err = ht.Sync()
	} else {
		err = errors.New(fmt.Sprintf("Hash table %s does not exist", name))
	}
	return
}

// Clear a hash table.
func (ds *DataSvc) HTClear(name string, _ *bool) (err error) {
	if ht, exists := ds.ht[name]; exists {
		err = ht.Clear()
	} else {
		err = errors.New(fmt.Sprintf("Hash table %s does not exist", name))
	}
	return
}

// Close a hash table file.
func (ds *DataSvc) HTClose(name string, _ *bool) (err error) {
	if ht, exists := ds.ht[name]; exists {
		err = ht.Close()
		delete(ds.ht, name)
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
type HTGetPartitionInput struct {
	Name                string
	PartNum, TotalParts int
	MySchemaVersion     int64
}
type HTGetPartitionOutput struct {
	Keys, Vals []int
}

func (ds *DataSvc) HTGetPartition(in HTGetPartitionInput, out *HTGetPartitionOutput) (err error) {
	if ht, exists := ds.ht[in.Name]; !exists {
		err = errors.New(fmt.Sprintf("Hash table %s does not exist", in.Name))
	} else if in.MySchemaVersion < ds.schemaVersion {
		err = errors.New(SCHEMA_VERSION_LOW)
	} else if in.PartNum < 0 || in.TotalParts < 1 || in.PartNum >= in.TotalParts {
		err = fmt.Errorf("Both partition number and total number should be positive, and partition number should be less than total")
	} else {
		keys, vals := ht.GetPartition(in.PartNum, in.TotalParts)
		out.Keys = keys
		out.Vals = vals
	}
	return
}
