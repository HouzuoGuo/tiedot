/* Document collection file. */
package file

import (
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"sync"
)

const (
	COL_FILE_GROWTH = uint64(134217728) // Grows every 128MB
	DOC_MAX_ROOM    = 33554432          // Maximum single document size
	DOC_HEADER      = 1 + 10            // byte(validity), uint64(document room)
	DOC_VALID       = byte(1)
	DOC_INVALID     = byte(0)
)

type ColFile struct {
	File          *File
	Padding       []byte
	syncDocInsert *sync.Mutex
	syncDocUpdate *sync.RWMutex
}

// Open a collection file.
func OpenCol(name string) (*ColFile, error) {
	file, err := Open(name, COL_FILE_GROWTH)
	// make an array of padding spaces
	padding := make([]byte, DOC_MAX_ROOM)
	for i := range padding {
		padding[i] = 32
	}
	return &ColFile{File: file, Padding: padding, syncDocInsert: new(sync.Mutex), syncDocUpdate: new(sync.RWMutex)}, err
}

// Retrieve document data given its ID.
func (col *ColFile) Read(id uint64) []byte {
	if id < 0 || id >= col.File.Append || col.File.Buf[id] != DOC_VALID {
		return nil
	}
	col.syncDocUpdate.RLock()
	if room, _ := binary.Uvarint(col.File.Buf[id+1 : id+11]); room > DOC_MAX_ROOM {
		col.syncDocUpdate.RUnlock()
		return nil
	} else {
		docCopy := make([]byte, room)
		copy(docCopy, col.File.Buf[id+DOC_HEADER:id+DOC_HEADER+room])
		col.syncDocUpdate.RUnlock()
		return docCopy
	}
}

// Insert a document, return its ID.
func (col *ColFile) Insert(data []byte) (id uint64, err error) {
	len64 := uint64(len(data))
	room := len64 + len64
	if room > DOC_MAX_ROOM {
		return 0, errors.New(fmt.Sprintf("Document is too large"))
	}
	col.syncDocInsert.Lock()
	id = col.File.Append
	// when file is full, we have lots to do
	col.File.CheckSizeAndEnsure(DOC_HEADER + room)
	col.File.Buf[id] = 1
	binary.PutUvarint(col.File.Buf[id+1:id+DOC_HEADER], room)
	copy(col.File.Buf[id+DOC_HEADER:id+DOC_HEADER+len64], data)
	copy(col.File.Buf[id+DOC_HEADER+len64:id+DOC_HEADER+room], col.Padding[0:len64])
	col.File.Append = id + DOC_HEADER + room
	col.syncDocInsert.Unlock()
	return id, nil
}

// Update a document, return its new ID.
func (col *ColFile) Update(id uint64, data []byte) (uint64, error) {
	if id < 0 || id >= col.File.Append || col.File.Buf[id] != DOC_VALID {
		return id, errors.New(fmt.Sprintf("Document %d does not exist in %s", id, col.File.Name))
	}
	len64 := uint64(len(data))
	col.syncDocUpdate.Lock()
	if room, _ := binary.Uvarint(col.File.Buf[id+1 : id+11]); room > DOC_MAX_ROOM {
		col.syncDocUpdate.Unlock()
		return id, errors.New(fmt.Sprintf("Document %d does not exist in %s", id, col.File.Name))
	} else {
		if len64 <= room { // overwrite
			copy(col.File.Buf[id+DOC_HEADER:id+DOC_HEADER+len64], data[:])
			copy(col.File.Buf[id+DOC_HEADER+len64:id+DOC_HEADER+room], col.Padding[0:room-len64])
			col.syncDocUpdate.Unlock()
			return id, nil
		}
		col.syncDocUpdate.Unlock()
		// re-insert
		col.Delete(id)
		return col.Insert(data)
	}
}

// Delete a document.
func (col *ColFile) Delete(id uint64) {
	if id >= 0 && id < col.File.Append && col.File.Buf[id] == DOC_VALID {
		col.File.Buf[id] = DOC_INVALID
	}
}

// Do fun for all documents in the collection.
func (col *ColFile) ForAll(fun func(id uint64, doc []byte) bool) {
	addr := uint64(0)
	col.syncDocUpdate.RLock()
	for {
		if addr >= col.File.Append {
			break
		}
		validity := col.File.Buf[addr]
		room, _ := binary.Uvarint(col.File.Buf[addr+1 : addr+11])
		if validity != DOC_VALID && validity != DOC_INVALID || room > DOC_MAX_ROOM {
			log.Printf("In %s at %d, the document is corrupted\n", col.File.Name, addr)
			// skip corrupted document
			addr++
			for ; col.File.Buf[addr] != DOC_VALID && col.File.Buf[addr] != DOC_INVALID; addr++ {
			}
			log.Printf("Corrupted document is skipped, now at %d\n", addr)
			continue
		}
		if validity == DOC_VALID && !fun(addr, col.File.Buf[addr+DOC_HEADER:addr+DOC_HEADER+room]) {
			break
		}
		addr += DOC_HEADER + room
	}
	col.syncDocUpdate.RUnlock()
}
