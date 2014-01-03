/* Coordination between a collection of chunks. */
package db

import (
	"errors"
	"fmt"
	"github.com/HouzuoGuo/tiedot/chunk"
	"github.com/HouzuoGuo/tiedot/chunkfile"
	"github.com/HouzuoGuo/tiedot/tdlog"
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
	ChunkMutexes  []*sync.Mutex // Synchronize access to chunks
	NewChunkMutex sync.Mutex    // Synchronize creation of new chunk
	NumChunks     uint64        // Total number of chunks
}

// Open a collection (made of chunks).
func OpenCol(baseDir string) (col *Col, err error) {
	// Create the directory if it does not yet exist
	if err = os.MkdirAll(baseDir, 0700); err != nil {
		return
	}
	col = &Col{BaseDir: baseDir, NewChunkMutex: sync.Mutex{},
		Chunks: make([]*chunk.ChunkCol, 0), ChunkMutexes: make([]*sync.Mutex, 0)}
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
			} else {
				tdlog.Errorf("Directory %s does not seem to belong to tiedot, it is ignored", info.Name())
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
		col.ChunkMutexes = append(col.ChunkMutexes, new(sync.Mutex))
	}
	return
}

// Insert a new document, return new document's ID.
func (col *Col) Insert(doc interface{}) (id uint64, err error) {
	// Try to insert the doc into a random chunk
	randChunkNum := uint64(rand.Int63n(int64(col.NumChunks)))
	randChunk := col.Chunks[randChunkNum]
	randChunkMutexes := col.ChunkMutexes[randChunkNum]
	randChunkMutexes.Lock()
	idInChunk, outOfSpace, err := randChunk.Insert(doc)
	if !outOfSpace {
		randChunkMutexes.Unlock()
		id = randChunkNum*chunkfile.COL_FILE_SIZE + idInChunk
		return
	}
	randChunkMutexes.Unlock()
	// If the random chunk was full, try again with the last chunk
	lastChunk := col.Chunks[col.NumChunks-1]
	lastChunkMutexes := col.ChunkMutexes[col.NumChunks-1]
	lastChunkMutexes.Lock()
	idInChunk, outOfSpace, err = lastChunk.Insert(doc)
	if !outOfSpace {
		id = (col.NumChunks-1)*chunkfile.COL_FILE_SIZE + idInChunk
		lastChunkMutexes.Unlock()
		return
	}
	lastChunkMutexes.Unlock()
	// If the last chunk is full, make a new chunk
	col.NewChunkMutex.Lock()
	tdlog.Printf("Going to create a new chunk (number %d) in collection %s", col.NumChunks, col.BaseDir)
	newChunk, err := chunk.OpenChunk(col.NumChunks, path.Join(col.BaseDir, strconv.Itoa(int(col.NumChunks))))
	col.NumChunks += 1
	if err != nil {
		col.NewChunkMutex.Unlock()
		return
	}
	// Put the new chunk into col structures
	col.Chunks = append(col.Chunks, newChunk)
	col.ChunkMutexes = append(col.ChunkMutexes, new(sync.Mutex))
	col.NewChunkMutex.Unlock()
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
	chunkMutex.Lock()
	err = chunk.Read(id, doc)
	chunkMutex.Unlock()
	return
}
