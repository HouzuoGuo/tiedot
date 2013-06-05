/* Document collection file. */
package file

import (
	"encoding/binary"
	"errors"
	"fmt"
)

const (
	COL_FILE_GROWTH = uint64(134217728) // Grows every 128MB
	DOC_MAX_ROOM    = 33554432          // Maximum single document size
	DOC_HEADER      = 1 + 8             // byte(validity), uint64(document room)
	DOC_VALID       = byte(1)
	DOC_INVALID     = byte(0)
)

type ColFile struct {
	File *File
}

// Open a collection file.
func OpenCol(name string) (*ColFile, error) {
	file, err := Open(name, COL_FILE_GROWTH)
	return &ColFile{File: file}, err
}

// Retrieve document data given its ID.
func (col *ColFile) Read(id uint64) []byte {
	if id < 0 || id > col.File.Append || col.File.Buf[id] != DOC_VALID {
		return nil
	}
	if room, _ := binary.Uvarint(col.File.Buf[id+1 : id+9]); room > DOC_MAX_ROOM {
		return nil
	} else {
		return col.File.Buf[id+DOC_HEADER : id+DOC_HEADER+room]
	}
}

// Insert a document, return its ID.
func (col *ColFile) Insert(data []byte) (uint64, error) {
	var (
		len64 = uint64(len(data))
		room  = len64 + len64
		id    = col.File.Append
	)
	if room > DOC_MAX_ROOM {
		return 0, errors.New(fmt.Sprintf("Document is too large"))
	}
	col.File.Ensure(DOC_HEADER + room)
	col.File.Buf[id] = 1
	binary.PutUvarint(col.File.Buf[id+1:id+DOC_HEADER], room)
	copy(col.File.Buf[id+DOC_HEADER:id+DOC_HEADER+len64], data)
	copy(col.File.Buf[id+DOC_HEADER+len64:id+DOC_HEADER+room], make([]byte, len64))
	col.File.Append = id + DOC_HEADER + room
	return id, nil
}

// Update a document, return its new ID.
func (col *ColFile) Update(id uint64, data []byte) (uint64, error) {
	if id < 0 || id > col.File.Append || col.File.Buf[id] != DOC_VALID {
		return id, errors.New(fmt.Sprintf("No such document %d in %s", id, col.File.Name))
	}
	if room, _ := binary.Uvarint(col.File.Buf[id+1 : id+9]); room > DOC_MAX_ROOM {
		return id, errors.New(fmt.Sprintf("No such document %d in %s", id, col.File.Name))
	} else {
		len64 := uint64(len(data))
		if len64 <= room { // Overwrite
			copy(col.File.Buf[id+DOC_HEADER:id+DOC_HEADER+len64], data)
			copy(col.File.Buf[id+DOC_HEADER+len64:id+DOC_HEADER+room], make([]byte, room-len64))
			return id, nil
		}
		// Re-insert
		col.Delete(id)
		return col.Insert(data)
	}
}

// Delete a document.
func (col *ColFile) Delete(id uint64) {
	if col.File.Buf[id] == DOC_VALID {
		col.File.Buf[id] = DOC_INVALID
	}
}
