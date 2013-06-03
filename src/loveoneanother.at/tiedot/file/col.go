/* A collection file. */
package file

import (
	"encoding/binary"
	"errors"
	"fmt"
)

const (
	COL_FILE_GROWTH = uint64(134217728)
	DOC_MAX_ROOM    = 33554432
	DOC_HEADER      = 9
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
func (col *ColFile) Read(id uint64) ([]byte, error) {
	if id < 0 || id > col.File.Append {
		return nil, errors.New(fmt.Sprintf("No such document %d in %s\n", id, col.File.Name))
	}
	switch col.File.Buf[id] {
	case DOC_INVALID:
		return nil, nil
	case DOC_VALID:
		if room, err := binary.Uvarint(col.File.Buf[id+1 : id+4]); err <= 0 || room < 0 || room > DOC_MAX_ROOM {
			return nil, errors.New(fmt.Sprintf("No such document %d in %s\n", id, col.File.Name))
		} else {
			return col.File.Buf[id+DOC_HEADER : id+DOC_HEADER+room], nil
		}
	default:
		return nil, errors.New(fmt.Sprintf("No such document %d in %s\n", id, col.File.Name))
	}
}

// Insert a document, return its ID.
func (col *ColFile) Insert(data []byte) (uint64, error) {
	var (
		len64      = uint64(len(data))
		room       = len64 + len64
		id         = col.File.Append
		dataBegin  = id + DOC_HEADER
		dataEnd    = dataBegin + len64
		paddingEnd = dataEnd + len64
	)
	if room > DOC_MAX_ROOM {
		return 0, errors.New(fmt.Sprintf("Document is too large"))
	}
	col.File.Ensure(DOC_HEADER + room)
	col.File.Buf[id] = 1
	binary.PutUvarint(col.File.Buf[id+1:dataBegin], room)
	copy(col.File.Buf[dataBegin:dataEnd], data)
	copy(col.File.Buf[dataEnd:paddingEnd], make([]byte, len(data)))
	col.File.Append = paddingEnd
	return id, nil
}

// Update a document, return its new ID.
func (col *ColFile) Update(id uint64, data []byte) (uint64, error) {
	if id < 0 || id > col.File.Append {
		return id, errors.New(fmt.Sprintf("No such document %d in %s\n", id, col.File.Name))
	}
	switch col.File.Buf[id] {
	case DOC_INVALID:
		return id, nil
	case DOC_VALID:
		if room, err := binary.Uvarint(col.File.Buf[id+1 : id+4]); err <= 0 || room < 0 || room > DOC_MAX_ROOM {
			return id, errors.New(fmt.Sprintf("No such document %d in %s\n", id, col.File.Name))
		} else {
			len64 := uint64(len(data))
			if len64 <= room { // Overwrite
				var (
					dataBegin = id + DOC_HEADER
					dataEnd   = dataBegin + len64
				)
				copy(col.File.Buf[dataBegin:dataEnd], data)
				copy(col.File.Buf[dataEnd:dataBegin+room], make([]byte, room-len64))
				return id, nil
			}
			// Re-insert
			col.Delete(id)
			return col.Insert(data)
		}
	default:
		return id, errors.New(fmt.Sprintf("No such document %d in %s\n", id, col.File.Name))
	}
}

// Delete a document.
func (col *ColFile) Delete(id uint64) {
	if col.File.Buf[id] == DOC_VALID {
		col.File.Buf[id] = DOC_INVALID
	}
}
