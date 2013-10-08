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

	// prepared document padding (2048 bytes of space)
	PADDING = "                                                                                                                                " +
		"                                                                                                                                " +
		"                                                                                                                                " +
		"                                                                                                                                " +
		"                                                                                                                                " +
		"                                                                                                                                " +
		"                                                                                                                                " +
		"                                                                                                                                "
	LEN_PADDING = uint64(len(PADDING))
)

type ColFile struct {
	File          *File
	syncDocInsert *sync.Mutex
	syncDocUpdate *MultiRWMutex
}

// Open a collection file.
func OpenCol(name string) (*ColFile, error) {
	file, err := Open(name, COL_FILE_GROWTH)
	return &ColFile{
		File:          file,
		syncDocInsert: new(sync.Mutex),
		syncDocUpdate: NewMultiRWMutex(8192)}, err
}

// Retrieve document data given its ID.
func (col *ColFile) Read(id uint64) []byte {
	if id < 0 || id >= col.File.Append || col.File.Buf[id] != DOC_VALID {
		return nil
	}
	mutex := col.syncDocUpdate.GetRWMutex(id)
	mutex.RLock()
	if room, _ := binary.Uvarint(col.File.Buf[id+1 : id+11]); room > DOC_MAX_ROOM {
		mutex.RUnlock()
		return nil
	} else {
		docCopy := make([]byte, room)
		copy(docCopy, col.File.Buf[id+DOC_HEADER:id+DOC_HEADER+room])
		mutex.RUnlock()
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
	if !col.File.CheckSize(DOC_HEADER + room) {
		col.syncDocUpdate.LockAll()
		col.File.CheckSizeAndEnsure(DOC_HEADER + room)
		col.syncDocUpdate.UnlockAll()
	}
	// reposition next append
	col.File.Append = id + DOC_HEADER + room
	col.syncDocInsert.Unlock()
	// make doc header and copy data
	col.File.Buf[id] = 1
	binary.PutUvarint(col.File.Buf[id+1:id+DOC_HEADER], room)
	paddingBegin := id + DOC_HEADER + len64
	paddingEnd := id + DOC_HEADER + room
	copy(col.File.Buf[id+DOC_HEADER:paddingBegin], data)
	// make padding
	for segBegin := paddingBegin; segBegin < paddingEnd; segBegin += LEN_PADDING {
		segSize := LEN_PADDING
		segEnd := segBegin + LEN_PADDING

		if segEnd >= paddingEnd {
			segEnd = paddingEnd
			segSize = paddingEnd - segBegin
		}
		copy(col.File.Buf[segBegin:segEnd], PADDING[0:segSize])
	}
	return id, nil
}

// Update a document, return its new ID.
func (col *ColFile) Update(id uint64, data []byte) (uint64, error) {
	if id < 0 || id >= col.File.Append || col.File.Buf[id] != DOC_VALID {
		return id, errors.New(fmt.Sprintf("Document %d does not exist in %s", id, col.File.Name))
	}
	len64 := uint64(len(data))
	mutex := col.syncDocUpdate.GetRWMutex(id)
	mutex.Lock()
	if room, _ := binary.Uvarint(col.File.Buf[id+1 : id+11]); room > DOC_MAX_ROOM {
		mutex.Unlock()
		return id, errors.New(fmt.Sprintf("Document %d does not exist in %s", id, col.File.Name))
	} else {
		if len64 <= room {
			// overwrite data
			paddingBegin := id + DOC_HEADER + len64
			copy(col.File.Buf[id+DOC_HEADER:paddingBegin], data)
			paddingEnd := id + DOC_HEADER + room
			// overwrite padding
			for segBegin := paddingBegin; segBegin < paddingEnd; segBegin += LEN_PADDING {
				segSize := LEN_PADDING
				segEnd := segBegin + LEN_PADDING

				if segEnd >= paddingEnd {
					segEnd = paddingEnd
					segSize = paddingEnd - segBegin
				}
				copy(col.File.Buf[segBegin:segEnd], PADDING[0:segSize])
			}
			mutex.Unlock()
			return id, nil
		}
		mutex.Unlock()
		// re-insert because there is not enough room
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
	for {
		if addr >= col.File.Append {
			break
		}
		mutex := col.syncDocUpdate.GetRWMutex(addr)
		mutex.RLock()
		validity := col.File.Buf[addr]
		room, _ := binary.Uvarint(col.File.Buf[addr+1 : addr+11])
		if validity != DOC_VALID && validity != DOC_INVALID || room > DOC_MAX_ROOM {
			mutex.RUnlock()
			log.Printf("In %s at %d, the document is corrupted\n", col.File.Name, addr)
			// skip corrupted document
			addr++
			for ; col.File.Buf[addr] != DOC_VALID && col.File.Buf[addr] != DOC_INVALID; addr++ {
			}
			log.Printf("Corrupted document is skipped, now at %d\n", addr)
			continue
		}
		if validity == DOC_VALID && !fun(addr, col.File.Buf[addr+DOC_HEADER:addr+DOC_HEADER+room]) {
			mutex.RUnlock()
			break
		}
		mutex.RUnlock()
		addr += DOC_HEADER + room
	}
}
