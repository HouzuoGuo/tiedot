/* Coordination between a collection of chunks. */
package db

import (
	"errors"
	"fmt"
	"github.com/HouzuoGuo/tiedot/chunk"
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
	HASHTABLE_DIRNAME_MAGIC = "ht_"    // Hash table directory name prefix
	CHUNK_DIRNAME_MAGIC     = "chunk_" // Chunk directory name prefix
	INDEX_PATH_SEP          = ","      // Separator between index path segments
)

type Col struct {
	BaseDir      string // Collection dir path
	Chunks       []*chunk.ChunkCol
	ChunkMutexes []*sync.RWMutex // Synchronize access to chunks
	NumChunks    int             // Total number of chunks
	NumChunksI64 uint64          // Total number of chunks (uint64)

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
func OpenCol(baseDir string, numChunks int) (col *Col, err error) {
	// Create the directory if it does not yet exist
	if err = os.MkdirAll(baseDir, 0700); err != nil {
		return
	}
	col = &Col{BaseDir: baseDir, NumChunks: numChunks, NumChunksI64: uint64(numChunks),
		SecIndexes: make(map[string][]*chunkfile.HashTable),
		Chunks:     make([]*chunk.ChunkCol, numChunks), ChunkMutexes: make([]*sync.RWMutex, numChunks)}
	// Open each chunk
	for i := 0; i < numChunks; i++ {
		col.Chunks[i], err = chunk.OpenChunk(i, path.Join(baseDir, CHUNK_DIRNAME_MAGIC+strconv.Itoa(int(i))))
		if err != nil {
			panic(err)
		}
		col.ChunkMutexes[i] = &sync.RWMutex{}
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
	for i := 0; i < col.NumChunks; i++ {
		if err := os.MkdirAll(baseDir, 0700); err != nil {
			panic(err)
		}
		table, err := chunkfile.OpenHash(path.Join(baseDir, strconv.Itoa(int(i))), indexPath)
		if err != nil {
			panic(err)
		}
		tables[i] = table
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
	col.ForAll(func(id int, doc map[string]interface{}) bool {
		for _, toBeIndexed := range GetIn(doc, indexPath) {
			if toBeIndexed != nil {
				// Figure out where to put it
				hash := chunk.StrHash(toBeIndexed)
				dest := hash % col.NumChunksI64
				newIndex[dest].Put(hash, uint64(id))
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
func (col *Col) indexDoc(id uint64, doc interface{}) {
	for _, index := range col.SecIndexes {
		for _, toBeIndexed := range GetIn(doc, index[0].Path) {
			if toBeIndexed != nil {
				// Figure out where to put it
				hashKey := chunk.StrHash(toBeIndexed)
				num := hashKey % col.NumChunksI64
				ht := index[num]
				ht.Mutex.Lock()
				index[num].Put(hashKey, id)
				ht.Mutex.Unlock()
			}
		}
	}
}

// Remove the document from all secondary indexes.
func (col *Col) unindexDoc(id uint64, doc interface{}) {
	for _, index := range col.SecIndexes {
		for _, toBeIndexed := range GetIn(doc, index[0].Path) {
			if toBeIndexed != nil {
				// Figure out where it was put
				hashKey := chunk.StrHash(toBeIndexed)
				num := hashKey % col.NumChunksI64
				ht := index[num]
				ht.Mutex.Lock()
				index[num].Remove(hashKey, id)
				ht.Mutex.Unlock()
			}
		}
	}
}

// Insert a document, return its unique ID.
func (col *Col) Insert(doc map[string]interface{}) (id int, err error) {
	// Allocate an ID to the document
	id = uid.NextUID()
	doc[uid.PK_NAME] = strconv.Itoa(id)
	num := id % col.NumChunks
	// Lock the chunk while inserting the document
	lock := col.ChunkMutexes[num]
	lock.Lock()
	if _, err = col.Chunks[num].Insert(doc); err != nil {
		lock.Unlock()
		return
	}
	col.indexDoc(uint64(id), doc)
	lock.Unlock()
	return
}

// Insert a document without allocating a new ID to it. Only for collection recovery operation.
func (col *Col) InsertRecovery(knownID int, doc map[string]interface{}) (err error) {
	doc[uid.PK_NAME] = strconv.Itoa(knownID)
	num := knownID % col.NumChunks
	// Lock the chunk while inserting the document
	lock := col.ChunkMutexes[num]
	lock.Lock()
	_, err = col.Chunks[num].Insert(doc)
	col.indexDoc(uint64(knownID), doc)
	lock.Unlock()
	return
}

// Read a document given its unique ID.
func (col *Col) Read(id int, doc interface{}) (physID uint64, err error) {
	num := id % col.NumChunks
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
func (col *Col) ReadNoLock(id int, doc interface{}) (physID uint64, err error) {
	num := id % col.NumChunks
	// Lock the chunk while reading the document
	dest := col.Chunks[num]
	physID, err = dest.GetPhysicalID(id)
	if err != nil {
		return
	}
	err = dest.Read(physID, doc)
	return
}

func (col *Col) HashScan(htPath string, key, limit uint64) (keys, vals []uint64) {
	num := key % col.NumChunksI64
	theIndex, exist := col.SecIndexes[htPath]
	if !exist {
		panic(fmt.Sprintf("Index %s does not exist", htPath))
	}
	theIndex[num].Mutex.RLock()
	keys, vals = theIndex[num].Get(key, limit)
	theIndex[num].Mutex.RUnlock()
	return
}

// Update a document given its unique ID.
func (col *Col) Update(id int, newDoc map[string]interface{}) (err error) {
	num := id % col.NumChunks
	lock := col.ChunkMutexes[num]
	dest := col.Chunks[num]
	newDoc[uid.PK_NAME] = strconv.Itoa(id)

	lock.Lock()
	// Read back the original document
	var oldDoc interface{}
	physID, err := col.ReadNoLock(id, &oldDoc)
	if err != nil {
		tdlog.Errorf("Original document %d cannot be readback, will try GetPhysicalID", id)
		physID, err = dest.GetPhysicalID(id)
		if err != nil {
			tdlog.Errorf("Failed to update %s, cannot find its physical ID", id)
			lock.Unlock()
			return err
		}
	}
	// Remove the original document from secondary indexes, and put new values into them
	col.unindexDoc(uint64(id), oldDoc)
	col.indexDoc(uint64(id), newDoc)
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
func (col *Col) Delete(id int) {
	num := id % col.NumChunks
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
	col.unindexDoc(uint64(id), oldDoc)
	dest.Delete(physID)
	lock.Unlock()
	return
}

/* Sequentially deserialize all documents and invoke the function on each document (Collection Scan).
The function must not write to this collection. */
func (col *Col) ForAll(fun func(id int, doc map[string]interface{}) bool) {
	numChunks := col.NumChunks
	for i := 0; i < numChunks; i++ {
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
	for i := 0; i < numChunks; i++ {
		dest := col.Chunks[i]
		lock := col.ChunkMutexes[i]
		lock.RLock()
		dest.DeserializeAll(template, fun)
		lock.RUnlock()
	}
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
