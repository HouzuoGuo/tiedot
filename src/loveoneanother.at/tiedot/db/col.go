/* Document collection. */
package db

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"loveoneanother.at/tiedot/file"
	"os"
	"path"
	"strings"
)

type IndexConf struct {
	FileName            string
	PerBucket, HashBits uint64
	IndexedPath         []string
}

type Config struct {
	Indexes []IndexConf
}

type Col struct {
	Data                *file.ColFile
	Config              *Config
	Dir, ConfigFileName string
	Indexes             map[IndexConf]*file.HashTable
	PathIndex           map[string]*file.HashTable
}

// Return string hash code.
func StrHash(thing interface{}) uint64 {
	str := fmt.Sprint(thing)
	length := len(str)
	hash := 0
	for i, c := range str {
		hash += int(c)*31 ^ (length - i)
	}
	return uint64(hash)
}

// Open a collection.
func OpenCol(dir string) (col *Col, err error) {
	if err = os.MkdirAll(dir, 0700); err != nil {
		return
	}
	col = &Col{ConfigFileName: path.Join(dir, "config"), Dir: dir}
	// make sure the config file exists
	tryOpen, err := os.OpenFile(col.ConfigFileName, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return
	} else if err = tryOpen.Close(); err != nil {
		return
	}
	// read index config
	config, err := ioutil.ReadFile(col.ConfigFileName)
	if err != nil {
		return
	}
	if string(config) == "" {
		col.Config = &Config{}
	} else if err = json.Unmarshal(config, &col.Config); err != nil {
		return
	}
	// open each index file
	for _, index := range col.Config.Indexes {
		ht, err := file.OpenHash(index.FileName, index.HashBits, index.PerBucket)
		if err != nil {
			return nil, err
		}
		col.PathIndex[strings.Join(index.IndexedPath, ",")] = ht
		col.Indexes[index] = ht
	}
	// open data file
	if col.Data, err = file.OpenCol(path.Join(dir, "data")); err != nil {
		return
	}
	return
}

// Get inside the data structure, along the given path.
func GetIn(doc interface{}, path []string) (thing interface{}) {
	thing = doc
	for _, seg := range path {
		switch t := thing.(type) {
		case bool:
			return nil
		case float64:
			return nil
		case string:
			return nil
		case nil:
			return nil
		case interface{}:
			thing = t.(map[string]interface{})[seg]
		default:
			return nil
		}
	}
	return thing
}

// Retrieve document data given its ID.
func (col *Col) Read(id uint64) (doc interface{}) {
	data := col.Data.Read(id)
	if data == nil {
		return
	}
	if err := json.Unmarshal(col.Data.Read(id), &doc); err != nil {
		fmt.Fprintf(os.Stderr, "Cannot parse document %d in %s to JSON\n", id, col.Dir)
	}
	return
}

// Index the document on all indexes
func (col *Col) IndexDoc(id uint64, doc interface{}) {
	completed := make(chan bool, len(col.Indexes))
	for k, v := range col.Indexes {
		go func() {
			v.Put(StrHash(GetIn(doc, k.IndexedPath)), id)
			completed <- true
		}()
	}
	for i := 0; i < len(col.Indexes); i++ {
		<-completed
	}
}

// Remove the document from all indexes
func (col *Col) UnindexDoc(id uint64, doc interface{}) {
	completed := make(chan bool, len(col.Indexes))
	for k, v := range col.Indexes {
		go func() {
			v.Remove(StrHash(GetIn(doc, k.IndexedPath)), 1, func(k, v uint64) bool {
				return v == id
			})
			completed <- true
		}()
	}
	for i := 0; i < len(col.Indexes); i++ {
		<-completed
	}
}

// Insert a new document.
func (col *Col) Insert(doc interface{}) (id uint64, err error) {
	data, err := json.Marshal(doc)
	if err != nil {
		return
	}
	if id, err = col.Data.Insert(data); err != nil {
		return
	}
	col.IndexDoc(id, doc)
	return
}

// Update a document, return its new ID.
func (col *Col) Update(id uint64, doc interface{}) (newID uint64, err error) {
	data, err := json.Marshal(doc)
	if err != nil {
		return
	}
	oldDoc := col.Read(id)
	if oldDoc == nil {
		return id, nil
	}
	completed := make(chan bool, 3)
	go func() {
		newID, err = col.Data.Update(id, data)
		completed <- true
	}()
	go func() {
		col.UnindexDoc(id, oldDoc)
		completed <- true
	}()
	go func() {
		col.UnindexDoc(id, doc)
		completed <- true
	}()
	<-completed
	<-completed
	<-completed
	return
}

// Delete a document.
func (col *Col) Delete(id uint64) {
	oldDoc := col.Read(id)
	if oldDoc == nil {
		return
	}
	completed := make(chan bool, 2)
	go func() {
		col.Data.Delete(id)
		completed <- true
	}()
	go func() {
		col.UnindexDoc(id, oldDoc)
		completed <- true
	}()
	<-completed
	<-completed
}

// Close a collection.
func (col *Col) Close() (err error) {
	if err = col.Data.File.Close(); err != nil {
		return
	}
	for _, ht := range col.Indexes {
		if err = ht.File.Close(); err != nil {
			return
		}
	}
	return
}
