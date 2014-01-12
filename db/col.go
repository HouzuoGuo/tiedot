/* Coordination between a collection of chunks. */
package db

import (
	"github.com/HouzuoGuo/tiedot/chunk"

	"errors"
	"fmt"
	"github.com/HouzuoGuo/tiedot/chunkfile"
	"github.com/HouzuoGuo/tiedot/tdlog"
	"github.com/HouzuoGuo/tiedot/uid"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

const (
	HASHTABLE_DIRNAME_MAGIC = "ht_" // Hash table directory name prefix
	INDEX_PATH_SEP          = ","   // Separator between index path segments
)

type Col struct {
	BaseDir      string // Collection dir path
	Chunks       []chunk.ChunkCol
	ChunkMutexes []sync.RWMutex // Synchronize access to chunks
	NumChunks    uint64         // Total number of chunks

	// Secondary indexes (hashtables)
	SecIndexes map[string][]*chunkfile.HashTable
}

// Resolve the attribute(s) in the document structure along the given path.
func GetIn(doc interface{}, path []string) (ret []interface{}) {
	docMap, ok := doc.(map[string]interface{})
	if !ok {
		tdlog.Printf("%v cannot be indexed because type conversation to map[string]interface{} failed", doc)
		return
	}
	var thing interface{} = docMap
	// Get into each path segment
	for i, seg := range path {
		if aMap, ok := thing.(map[string]interface{}); ok {
			thing = aMap[seg]
		} else if anArray, ok := thing.([]interface{}); ok {
			for _, element := range anArray {
				ret = append(ret, GetIn(element, path[i:])...)
			}
			return ret
		} else {
			return nil
		}
	}
	switch thing.(type) {
	case []interface{}:
		return append(ret, thing.([]interface{})...)
	default:
		return append(ret, thing)
	}
}

// Open a collection (made of chunks).
func OpenCol(baseDir string, numChunks uint64) (col Col, err error) {
	// Create the directory if it does not yet exist
	if err = os.MkdirAll(baseDir, 0700); err != nil {
		return
	}
	col = Col{BaseDir: baseDir, NumChunks: numChunks, SecIndexes: make(map[string][]*chunkfile.HashTable),
		Chunks: make([]chunk.ChunkCol, numChunks), ChunkMutexes: make([]sync.RWMutex, numChunks)}
	// Open each chunk
	for i := uint64(0); i < numChunks; i++ {
		col.Chunks[i], err = chunk.OpenChunk(i, path.Join(baseDir, strconv.Itoa(int(i))))
		if err != nil {
			panic(err)
		}
		col.ChunkMutexes[i] = sync.RWMutex{}
	}
	// Look for hash table directories
	walker := func(currPath string, info os.FileInfo, err2 error) error {
		if err2 != nil {
			// log and skip the error
			tdlog.Error(err)
			return nil
		}
		if info.IsDir() {
			switch {
			case strings.HasPrefix(info.Name(), HASHTABLE_DIRNAME_MAGIC):
				// Found a hashtable index
				tdlog.Printf("Opening collection index hashtable %s", info.Name())
				// Figure out indexed path
				indexPath := strings.Split(info.Name()[len(HASHTABLE_DIRNAME_MAGIC):], INDEX_PATH_SEP)
				// Open a hash table index and put it into collection structure
				col.openIndex(indexPath, path.Join(baseDir, info.Name()))
			}
		}
		return nil
	}
	err = filepath.Walk(baseDir, walker)
	return
}

// Open a hash table index and put it into collection structure.
func (col *Col) openIndex(indexPath []string, baseDir string) {
	jointPath := strings.Join(indexPath, INDEX_PATH_SEP)
	tables := make([]*chunkfile.HashTable, col.NumChunks)
	for i := uint64(0); i < col.NumChunks; i++ {
		if err := os.MkdirAll(baseDir, 0700); err != nil {
			panic(err)
		}
		table, err := chunkfile.OpenHash(path.Join(baseDir, strconv.Itoa(int(i))), indexPath)
		if err != nil {
			panic(err)
		}
		tables[i] = &table
	}
	col.SecIndexes[jointPath] = tables
}

// Create a new index.
func (col *Col) Index(indexPath []string) error {
	jointPath := strings.Join(indexPath, INDEX_PATH_SEP)
	// Check whether the index already exists
	if _, alreadyExists := col.SecIndexes[jointPath]; alreadyExists {
		return errors.New(fmt.Sprintf("Path %v is already indexed in collection %s", indexPath, col.BaseDir))
	}
	// Make the new index
	indexBaseDir := path.Join(col.BaseDir, HASHTABLE_DIRNAME_MAGIC+jointPath)
	col.openIndex(indexPath, indexBaseDir)
	// Put all documents on the new index
	newIndex := col.SecIndexes[jointPath]
	col.ForAll(func(id string, doc map[string]interface{}) bool {
		for _, toBeIndexed := range GetIn(doc, indexPath) {
			if toBeIndexed != nil {
				// Figure out where to put it
				hash := chunk.StrHash(toBeIndexed)
				dest := hash % col.NumChunks
				newIndex[dest].Put(hash, chunk.StrHash(id))
			}
		}
		return true
	})
	return nil
}

// Remove a secondary index.
func (col *Col) Unindex(indexPath []string) (err error) {
	jointPath := strings.Join(indexPath, ",")
	if _, found := col.SecIndexes[jointPath]; !found {
		return errors.New(fmt.Sprintf("Path %v is not indexed in collection %s", indexPath, col.BaseDir))
	}
	// Close the index file
	for _, indexFile := range col.SecIndexes[jointPath] {
		indexFile.File.Close()
	}
	if err = os.RemoveAll(path.Join(col.BaseDir, HASHTABLE_DIRNAME_MAGIC+jointPath)); err != nil {
		return
	}
	// Remove the index from collection structure
	delete(col.SecIndexes, jointPath)
	return nil
}

// Put the document on all secondary indexes.
func (col *Col) indexDoc(id string, doc interface{}) {
	for _, index := range col.SecIndexes {
		for _, toBeIndexed := range GetIn(doc, index[0].Path) {
			if toBeIndexed != nil {
				// Figure out where to put it
				hash := chunk.StrHash(toBeIndexed)
				dest := hash % col.NumChunks
				index[dest].Put(hash, chunk.StrHash(id))
			}
		}
	}
}

// Remove the document from all secondary indexes.
func (col *Col) unindexDoc(id string, doc interface{}) {
	for _, index := range col.SecIndexes {
		for _, toBeIndexed := range GetIn(doc, index[0].Path) {
			if toBeIndexed != nil {
				// Figure out where it was put
				hash := chunk.StrHash(toBeIndexed)
				dest := hash % col.NumChunks
				index[dest].Remove(hash, chunk.StrHash(id))
			}
		}
	}
}

// Insert a document, return its unique ID.
func (col *Col) Insert(doc map[string]interface{}) (id string, err error) {
	// Allocate an ID to the document
	id = uid.NextUID()
	idHash := chunk.StrHash(id)
	doc[chunk.PK_NAME] = id
	num := idHash % col.NumChunks
	// Lock the chunk while inserting the document
	lock := col.ChunkMutexes[num]
	lock.Lock()
	_, err = col.Chunks[num].Insert(doc)
	col.indexDoc(id, doc)
	lock.Unlock()
	return
}

// Read a document given its unique ID.
func (col *Col) Read(id string, doc interface{}) (physID uint64, err error) {
	idHash := chunk.StrHash(id)
	num := idHash % col.NumChunks
	// Lock the chunk while reading the document
	lock := col.ChunkMutexes[num]
	dest := col.Chunks[num]
	lock.RLock()
	physID, err = dest.GetPhysicalID(id)
	if err != nil {
		lock.RUnlock()
		return
	}
	err = dest.Read(physID, doc)
	lock.RUnlock()
	return
}

// Read a document given its unique ID (without placing any lock).
func (col *Col) ReadNoLock(id string, doc interface{}) (physID uint64, err error) {
	idHash := chunk.StrHash(id)
	num := idHash % col.NumChunks
	// Lock the chunk while reading the document
	dest := col.Chunks[num]
	physID, err = dest.GetPhysicalID(id)
	if err != nil {
		return
	}
	err = dest.Read(physID, doc)
	return
}

func (col *Col) HashScan(htPath string, key, limit uint64, filter func(uint64, uint64) bool) (keys, vals []uint64) {
	num := key % col.NumChunks
	lock := col.ChunkMutexes[num]
	// Although index is not maintained by chunk, but we lock it from other modifications
	theIndex, exist := col.SecIndexes[htPath]
	if !exist {
		panic(fmt.Sprintf("Index %s does not exist", htPath))
	}
	lock.RLock()
	keys, vals = theIndex[num].Get(key, limit, filter)
	lock.RUnlock()
	return
}

// Update a document given its unique ID.
func (col *Col) Update(id string, newDoc map[string]interface{}) (err error) {
	idHash := chunk.StrHash(id)
	num := idHash % col.NumChunks
	lock := col.ChunkMutexes[num]
	dest := col.Chunks[num]
	newDoc[chunk.PK_NAME] = id

	lock.Lock()
	// Read back the original document
	var oldDoc interface{}
	physID, err := col.ReadNoLock(id, &oldDoc)
	if err != nil {
		tdlog.Errorf("Original document %s cannot be readback, will try GetPhysicalID", id)
		physID, err = dest.GetPhysicalID(id)
		if err != nil {
			tdlog.Errorf("Failed to update %s, cannot find its physical ID", id)
			lock.Unlock()
			return err
		}
	}
	// Remove the original document from secondary indexes, and put new values into them
	col.unindexDoc(id, oldDoc)
	col.indexDoc(id, newDoc)
	// Update document data file and return
	_, err = dest.Update(physID, newDoc)
	if err != nil {
		lock.Unlock()
		return
	}
	lock.Unlock()
	return
}

// Delete a document given its unique ID.
func (col *Col) Delete(id string) {
	idHash := chunk.StrHash(id)
	num := idHash % col.NumChunks
	lock := col.ChunkMutexes[num]
	dest := col.Chunks[num]
	lock.Lock()

	// Read back the original document
	var oldDoc interface{}
	physID, err := col.ReadNoLock(id, &oldDoc)
	if err != nil {
		physID, err = dest.GetPhysicalID(id)
		if err != nil {
			lock.Unlock()
			return
		}
	}
	// Remove the original document from secondary indexes
	col.unindexDoc(id, oldDoc)
	dest.Delete(physID)
	lock.Unlock()
	return
}

/* Sequentially deserialize all documents and invoke the function on each document (Collection Scan).
The function must not write to this collection. */
func (col *Col) ForAll(fun func(id string, doc map[string]interface{}) bool) {
	numChunks := col.NumChunks
	for i := uint64(0); i < numChunks; i++ {
		dest := col.Chunks[i]
		lock := col.ChunkMutexes[i]
		lock.RLock()
		dest.ForAll(fun)
		lock.RUnlock()
	}
}

/* Sequentially deserialize all documents into the template (pointer to struct) and invoke the function on each document (Collection Scan).
The function must not write to this collection. */
func (col *Col) DeserializeAll(template interface{}, fun func() bool) {
	numChunks := col.NumChunks
	for i := uint64(0); i < numChunks; i++ {
		dest := col.Chunks[i]
		lock := col.ChunkMutexes[i]
		lock.RLock()
		dest.DeserializeAll(template, fun)
		lock.RUnlock()
	}
}

// Compact the collection and automatically repair any data/index damage.
func (col *Col) Scrub() (recovered uint64) {
	for i, dest := range col.Chunks {
		// Do it one by one
		lock := col.ChunkMutexes[i]
		lock.Lock()
		for _, index := range col.SecIndexes {
			index[i].Clear()
		}
		recovered += dest.Scrub()
		lock.Unlock()
	}
	return
}

// Flush collection data and index files.
func (col *Col) Flush() error {
	// Close chunks
	for _, dest := range col.Chunks {
		if err := dest.Flush(); err != nil {
			return err
		}
	}
	// Flush secondary indexes
	for _, index := range col.SecIndexes {
		for _, part := range index {
			if err := part.File.Flush(); err != nil {
				return err
			}
		}
	}
	tdlog.Printf("Collection %s has all buffers flushed", col.BaseDir)
	return nil
}

// Close the collection.
func (col *Col) Close() {
	// Close chunks
	for _, dest := range col.Chunks {
		dest.Close()
	}
	// Close secondary indexes
	for _, index := range col.SecIndexes {
		for _, part := range index {
			part.File.Close()
		}
	}
	tdlog.Printf("Collection %s is closed", col.BaseDir)
}
