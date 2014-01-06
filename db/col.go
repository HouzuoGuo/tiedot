/* Coordination between a collection of chunks. */
package db

import (
	"errors"
	"fmt"
	"github.com/HouzuoGuo/tiedot/chunk"
	"github.com/HouzuoGuo/tiedot/chunkfile"
	"github.com/HouzuoGuo/tiedot/tdlog"
	"github.com/HouzuoGuo/tiedot/uid"
	"math/rand"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"sync"
)

type Col struct {
	BaseDir       string // Collection dir path
	Chunks        []*chunk.ChunkCol
	ChunkMutexes  []*sync.RWMutex // Synchronize access to chunks
	NewChunkMutex sync.Mutex      // Synchronize creation of new chunk
	NumChunks     uint64          // Total number of chunks
}

// Open a collection (made of chunks).
func OpenCol(baseDir string) (col *Col, err error) {
	// Create the directory if it does not yet exist
	if err = os.MkdirAll(baseDir, 0700); err != nil {
		return
	}
	col = &Col{BaseDir: baseDir, NewChunkMutex: sync.Mutex{},
		Chunks: make([]*chunk.ChunkCol, 0), ChunkMutexes: make([]*sync.RWMutex, 0)}
	// Walk the collection directory and look for how many chunks there are
	maxChunk := 0
	walker := func(currPath string, info os.FileInfo, err2 error) error {
		if err2 != nil {
			// log and skip the error
			tdlog.Error(err)
			return nil
		}
		if info.IsDir() {
			// According to the directory name - chunk number, look for how many chunks there are
			if chunkNum, err := strconv.Atoi(info.Name()); err == nil {
				if chunkNum > maxChunk {
					maxChunk = chunkNum
				}
			}
			return nil
		}
		return nil
	}
	if err = filepath.Walk(baseDir, walker); err != nil {
		return
	}
	col.NumChunks = uint64(maxChunk) + 1
	// Open all chunks - there is at least one chunk
	tdlog.Printf("Opening %d chunks in collection %s", maxChunk+1, baseDir)
	for i := 0; i <= maxChunk; i++ {
		var oneChunk *chunk.ChunkCol
		oneChunk, err = chunk.OpenChunk(uint64(i), path.Join(baseDir, strconv.Itoa(i)))
		col.Chunks = append(col.Chunks, oneChunk)
		col.ChunkMutexes = append(col.ChunkMutexes, new(sync.RWMutex))
	}
	return
}

// Create a new chunk.
func (col *Col) CreateNewChunk() {
	col.NewChunkMutex.Lock()
	defer col.NewChunkMutex.Unlock()
	tdlog.Printf("Going to create a new chunk (number %d) in collection %s", col.NumChunks, col.BaseDir)
	newChunk, err := chunk.OpenChunk(col.NumChunks, path.Join(col.BaseDir, strconv.Itoa(int(col.NumChunks))))
	if err != nil {
		col.NewChunkMutex.Unlock()
		return
	}
	// Make indexes
	for _, path := range col.Chunks[0].HTPaths {
		if path[0] != chunk.UID_PATH {
			if err := newChunk.Index(path); err != nil {
				tdlog.Panicf("Failed to create index %s, error: %v", path, err)
			}
		}
	}
	// Put the new chunk into col structures
	col.Chunks = append(col.Chunks, newChunk)
	col.ChunkMutexes = append(col.ChunkMutexes, new(sync.RWMutex))
	col.NumChunks += 1
}

// Insert a new document, return new document's ID.
func (col *Col) Insert(doc interface{}) (id uint64, err error) {
	// Try to insert the doc into a random chunk
	randChunkNum := uint64(rand.Int63n(int64(col.NumChunks)))
	randChunk := col.Chunks[randChunkNum]
	randChunkMutex := col.ChunkMutexes[randChunkNum]
	randChunkMutex.Lock()
	id, outOfSpace, err := randChunk.Insert(doc)
	if !outOfSpace {
		randChunkMutex.Unlock()
		return
	}
	randChunkMutex.Unlock()
	// If the random chunk was full, try again with the last chunk
	lastChunk := col.Chunks[col.NumChunks-1]
	lastChunkMutex := col.ChunkMutexes[col.NumChunks-1]
	lastChunkMutex.Lock()
	id, outOfSpace, err = lastChunk.Insert(doc)
	if !outOfSpace {
		lastChunkMutex.Unlock()
		return
	}
	lastChunkMutex.Unlock()
	// If the last chunk is full, make a new chunk
	col.CreateNewChunk()
	// Now there is a new chunk, let us try again
	return col.Insert(doc)
}

// Read document at the given ID.
func (col *Col) Read(id uint64, doc interface{}) (err error) {
	chunkNum := id / chunkfile.COL_FILE_SIZE
	if chunkNum >= col.NumChunks {
		return errors.New(fmt.Sprintf("Document %d does not exist in %s - out of bound chunk", id, col.BaseDir))
	}
	chunk := col.Chunks[chunkNum]
	chunkMutex := col.ChunkMutexes[chunkNum]
	chunkMutex.RLock()
	err = chunk.Read(id, doc)
	chunkMutex.RUnlock()
	return
}

// Update a document, return its new ID.
func (col *Col) Update(id uint64, doc interface{}) (newID uint64, err error) {
	chunkNum := id / chunkfile.COL_FILE_SIZE
	if chunkNum >= col.NumChunks {
		err = errors.New(fmt.Sprintf("Document %d does not exist in %s - out of bound chunk", id, col.BaseDir))
		return
	}
	chunk := col.Chunks[chunkNum]
	chunkMutex := col.ChunkMutexes[chunkNum]
	chunkMutex.Lock()
	newID, outOfSpace, err := chunk.Update(id, doc)
	chunkMutex.Unlock()
	if !outOfSpace {
		newID += chunkNum * chunkfile.COL_FILE_SIZE
		return
	}
	tdlog.Printf("Out of space")
	// The chunk does not have enough space for the updated document, let us put it somewhere else
	// The document has already been removed from its original chunk
	return col.Insert(doc)
}

// Delete a document by ID.
func (col *Col) Delete(id uint64) {
	chunkNum := id / chunkfile.COL_FILE_SIZE
	if chunkNum >= col.NumChunks {
		return
	}
	chunk := col.Chunks[chunkNum]
	chunkMutex := col.ChunkMutexes[chunkNum]
	chunkMutex.Lock()
	chunk.Delete(id)
	chunkMutex.Unlock()
}

// Create an index on the path.
func (col *Col) Index(path []string) (err error) {
	// Do not allow new chunk creation for now
	col.NewChunkMutex.Lock()
	defer col.NewChunkMutex.Unlock()
	for _, chunk := range col.Chunks {
		if err = chunk.Index(path); err != nil {
			return
		}
	}
	return
}

// Remove an index.
func (col *Col) Unindex(path []string) (err error) {
	// Do not allow new chunk creation for now
	col.NewChunkMutex.Lock()
	defer col.NewChunkMutex.Unlock()
	for _, chunk := range col.Chunks {
		if err = chunk.Unindex(path); err != nil {
			return
		}
	}
	return
}

// Insert a new document, and assign it a UID.
func (col *Col) InsertWithUID(doc interface{}) (newID uint64, newUID string, err error) {
	newUID = uid.NextUID()
	if docMap, ok := doc.(map[string]interface{}); !ok {
		err = errors.New("Only JSON object document may have UID")
		return
	} else {
		docMap[chunk.UID_PATH] = newUID
		newID, err = col.Insert(doc)
		return
	}
}

// Hash scan across all chunks.
func (col *Col) HashScan(htPath string, key, limit uint64, filter func(uint64, uint64) bool) (keys, vals []uint64) {
	keys = make([]uint64, 0)
	vals = make([]uint64, 0)
	numChunks := col.NumChunks
	for i := uint64(0); i < numChunks; i++ {
		chunk := col.Chunks[i]
		ht := chunk.Path2HT[htPath]
		k, v := ht.Get(key, limit, filter)
		keys = append(keys, k...)
		vals = append(vals, v...)
		if limit > 0 && uint64(len(keys)) >= limit {
			break
		}
	}
	if limit == 0 || uint64(len(keys)) <= limit {
		return
	} else {
		// Return only `limit` number of results
		return keys[:limit], vals[:limit]
	}
}

// Retrieve documentby UID, return its ID.
func (col *Col) ReadByUID(uid string, doc interface{}) (id uint64, err error) {
	var docID uint64
	found := false
	// Scan UID hash table, find potential matches
	col.HashScan(chunk.UID_PATH, chunk.StrHash(uid), 1, func(key, value uint64) bool {
		var candidate interface{}
		if col.Read(value, &candidate) == nil {
			if docMap, ok := candidate.(map[string]interface{}); ok {
				// Physically read the document to avoid hash collision
				if candidateUID, ok := docMap[chunk.UID_PATH]; ok {
					if stringUID, ok := candidateUID.(string); ok {
						if stringUID != uid {
							return false // A hash collision
						}
						docID = value
						found = true
					}
				}
			}
		}
		return true
	})
	if !found {
		return 0, errors.New(fmt.Sprintf("Document %s does not exist in %s", uid, col.BaseDir))
	}
	return docID, col.Read(docID, doc)
}

// Identify a document using UID and update it, return its new ID.
func (col *Col) UpdateByUID(uid string, doc interface{}) (newID uint64, err error) {
	var throwAway interface{}
	if newID, err = col.ReadByUID(uid, &throwAway); err != nil {
		return
	} else {
		return col.Update(newID, doc)
	}
}

// Give a document (identified by ID) a new UID.
func (col *Col) ReassignUID(id uint64) (newID uint64, newUID string, err error) {
	newUID = uid.NextUID()
	var originalDoc interface{}
	if err = col.Read(id, &originalDoc); err != nil {
		return
	}
	if docWithUID, ok := originalDoc.(map[string]interface{}); !ok {
		err = errors.New("Only JSON object document may have UID")
		return
	} else {
		docWithUID[chunk.UID_PATH] = newUID
		newID, err = col.Update(id, docWithUID)
		return
	}
	return
}

// Delete a document by UID.
func (col *Col) DeleteByUID(uid string) bool {
	var throwAway interface{}
	if id, err := col.ReadByUID(uid, &throwAway); err == nil {
		col.Delete(id)
		return true
	}
	return false
}

/* Sequentially deserialize all documents and invoke the function on each document (Collection Scan).
The function must not write to this collection. */
func (col *Col) ForAll(fun func(id uint64, doc interface{}) bool) {
	numChunks := col.NumChunks
	for i := uint64(0); i < numChunks; i++ {
		chunk := col.Chunks[i]
		chunkMutex := col.ChunkMutexes[i]
		chunkMutex.RLock()
		chunk.ForAll(fun)
		chunkMutex.RUnlock()
	}
}

/* Sequentially deserialize all documents into the template (pointer to struct) and invoke the function on each document (Collection Scan).
The function must not write to this collection. */
func (col *Col) DeserializeAll(template interface{}, fun func(id uint64) bool) {
	numChunks := col.NumChunks
	for i := uint64(0); i < numChunks; i++ {
		chunk := col.Chunks[i]
		chunkMutex := col.ChunkMutexes[i]
		chunkMutex.RLock()
		chunk.DeserializeAll(template, fun)
		chunkMutex.RUnlock()
	}
}

// Compact the collection and automatically repair any data/index damage.
func (col *Col) Scrub() (recovered uint64) {
	// Do not allow new chunk creation for now
	col.NewChunkMutex.Lock()
	defer col.NewChunkMutex.Unlock()
	for _, chunk := range col.Chunks {
		recovered += chunk.Scrub()
	}
	return
}

// Flush collection data and index files.
func (col *Col) Flush() error {
	// Do not allow new chunk creation for now
	col.NewChunkMutex.Lock()
	defer col.NewChunkMutex.Unlock()
	for _, chunk := range col.Chunks {
		if err := chunk.Flush(); err != nil {
			return err
		}
	}
	return nil
}

// Close the collection.
func (col *Col) Close() {
	for _, chunk := range col.Chunks {
		chunk.Close()
	}
}
