/* Document collection data file. */
package file

import (
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"sync"
)

const (
	COL_FILE_GROWTH      = uint64(134217728) // Grows by 128MB
	DOC_MAX_ROOM         = 33554432          // Maximum size of a single document (initial size is halved)
	DOC_HEADER           = 1 + 10            // Document header: validity (byte), document room (uint64)
	DOC_VALID            = byte(1)           // Document valid flag
	DOC_INVALID          = byte(0)           // Document invalid flag
	COL_FILE_REGION_SIZE = 1024 * 64         // Granulairty of locks

	// Document padding made of spaces (pre-compiled, 2048 bytes)
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
	File           *File
	docInsertMutex sync.Mutex      // Lock for inserting documents
	regionRWMutex  []*sync.RWMutex // Lock for modifying document data (one lock per region)
}

// Open a collection file.
func OpenCol(name string) (*ColFile, error) {
	file, err := Open(name, COL_FILE_GROWTH)
	// Devide collection file into regions, make one RW lock per region
	rwMutexes := make([]*sync.RWMutex, file.Size/COL_FILE_REGION_SIZE)
	for i := range rwMutexes {
		rwMutexes[i] = new(sync.RWMutex)
	}
	return &ColFile{
		File:           file,
		docInsertMutex: sync.Mutex{},
		regionRWMutex:  rwMutexes}, err
}

// Retrieve document data given its ID.
func (col *ColFile) Read(id uint64) []byte {
	if col.File.UsedSize < DOC_HEADER || id >= col.File.UsedSize-DOC_HEADER {
		return nil
	}
	region := id / COL_FILE_REGION_SIZE
	mutex := col.regionRWMutex[region]
	mutex.RLock()
	if col.File.Buf[id] != DOC_VALID {
		mutex.RUnlock()
		return nil
	}
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
	col.docInsertMutex.Lock()
	id = col.File.UsedSize
	// When there is not enough room for new document...
	if !col.File.CheckSize(DOC_HEADER + room) {
		// Lock down all regions
		originalMutexes := col.regionRWMutex
		for _, region := range originalMutexes {
			region.Lock()
		}
		// Grow the data file, and make more mutexes for the new space
		col.File.CheckSizeAndEnsure(DOC_HEADER + room)
		moreMutexes := make([]*sync.RWMutex, COL_FILE_GROWTH/COL_FILE_REGION_SIZE+1)
		for i := range moreMutexes {
			moreMutexes[i] = new(sync.RWMutex)
		}
		// Put original and new mutexes together
		col.regionRWMutex = append(col.regionRWMutex, moreMutexes...)
		for _, region := range originalMutexes {
			region.Unlock()
		}
	}
	col.File.UsedSize = id + DOC_HEADER + room
	// Make document header (document valid (byte), occupied room (uint64)) and copy document content
	col.File.Buf[id] = 1
	binary.PutUvarint(col.File.Buf[id+1:id+DOC_HEADER], room)
	paddingBegin := id + DOC_HEADER + len64
	copy(col.File.Buf[id+DOC_HEADER:paddingBegin], data)
	// Make padding - fill up the padded space
	paddingEnd := id + DOC_HEADER + room
	for segBegin := paddingBegin; segBegin < paddingEnd; segBegin += LEN_PADDING {
		segSize := LEN_PADDING
		segEnd := segBegin + LEN_PADDING

		if segEnd >= paddingEnd {
			segEnd = paddingEnd
			segSize = paddingEnd - segBegin
		}
		copy(col.File.Buf[segBegin:segEnd], PADDING[0:segSize])
	}
	col.docInsertMutex.Unlock()
	return id, nil
}

// Update a document, return its new ID.
func (col *ColFile) Update(id uint64, data []byte) (uint64, error) {
	if col.File.UsedSize < DOC_HEADER || id >= col.File.UsedSize-DOC_HEADER {
		return id, errors.New(fmt.Sprintf("Document %d does not exist in %s", id, col.File.Name))
	}
	len64 := uint64(len(data))
	region := id / COL_FILE_REGION_SIZE
	mutex := col.regionRWMutex[region]
	mutex.Lock()
	if col.File.Buf[id] != DOC_VALID {
		mutex.Unlock()
		return id, errors.New(fmt.Sprintf("Document %d does not exist in %s", id, col.File.Name))
	}
	if room, _ := binary.Uvarint(col.File.Buf[id+1 : id+11]); room > DOC_MAX_ROOM {
		mutex.Unlock()
		return id, errors.New(fmt.Sprintf("Document %d does not exist in %s", id, col.File.Name))
	} else {
		if len64 <= room {
			// There is enough room for updated content
			// Overwrite document content
			paddingBegin := id + DOC_HEADER + len64
			copy(col.File.Buf[id+DOC_HEADER:paddingBegin], data)
			// Overwrite padding
			paddingEnd := id + DOC_HEADER + room
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
		// There is not enough room for updated content, so delete the original document and re-insert
		mutex.Unlock()
		col.Delete(id)
		return col.Insert(data)
	}
}

// Delete a document.
func (col *ColFile) Delete(id uint64) {
	if col.File.UsedSize < DOC_HEADER || id >= col.File.UsedSize-DOC_HEADER {
		return
	}
	region := id / COL_FILE_REGION_SIZE
	mutex := col.regionRWMutex[region]
	mutex.Lock()
	if col.File.Buf[id] == DOC_VALID {
		col.File.Buf[id] = DOC_INVALID
	}
	mutex.Unlock()
}

// Scan the entire data file, look for documents and invoke the function on each.
func (col *ColFile) ForAll(fun func(id uint64, doc []byte) bool) {
	addr := uint64(0)
	for {
		if col.File.UsedSize < DOC_HEADER || addr >= col.File.UsedSize-DOC_HEADER {
			break
		}
		// Lock down document region
		region := addr / COL_FILE_REGION_SIZE
		mutex := col.regionRWMutex[region]
		mutex.RLock()
		// Read document header - validity and room
		validity := col.File.Buf[addr]
		room, _ := binary.Uvarint(col.File.Buf[addr+1 : addr+11])
		if validity != DOC_VALID && validity != DOC_INVALID || room > DOC_MAX_ROOM {
			// If the document does not contain valid header, skip it
			mutex.RUnlock()
			log.Printf("ERROR: The document at %d in %s is corrupted", addr, col.File.Name)
			// Move forward until we meet a valid document header
			for addr++; col.File.Buf[addr] != DOC_VALID && col.File.Buf[addr] != DOC_INVALID && addr < col.File.UsedSize-DOC_HEADER; addr++ {
			}
			continue
		}
		// If the function returns false, do not continue scanning
		if validity == DOC_VALID && !fun(addr, col.File.Buf[addr+DOC_HEADER:addr+DOC_HEADER+room]) {
			mutex.RUnlock()
			break
		}
		mutex.RUnlock()
		addr += DOC_HEADER + room
	}
}
