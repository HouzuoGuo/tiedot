package data

import (
	"encoding/binary"
	"errors"
	"fmt"
)

type DocFile struct {
	File *File
}

const (
	DOC_FILE_GROWTH = uint64(67108864)
	DOC_MAX_SIZE    = uint64(16777216)
	DOC_MAX_ROOM    = uint64(16777216 * 2)
	DOC_HEADER      = uint64(9) // validity (uint8), room (uint64)
	DOC_VALID       = byte(1)
	DOC_INVALID     = byte(0)
)

func OpenCollection(name string, growth uint64) (file *DocFile, err error) {
	o_file, err := Open(name, growth)
	if err != nil {
		return
	}
	file = &DocFile{File: o_file}
	return
}

// Retrieve and return document given its ID
func (file *DocFile) Read(id uint64) (data []byte, err error) {
	if id < 0 || id > file.File.Append {
		err = errors.New(fmt.Sprintf("No such document (%d) in collection %s", id, file.File.Name))
		return
	}
	if file.File.Buf[id] == DOC_INVALID {
		return
	}
	if file.File.Buf[id] != DOC_VALID {
		err = errors.New(fmt.Sprintf("No such document (%d) in collection %s", id, file.File.Name))
		return
	}
	roomI64, ierr := binary.Varint(file.File.Buf[id+1 : id+4])
	room := uint64(roomI64)
	if ierr <= 0 || room < 0 || room > DOC_MAX_ROOM {
		err = errors.New(fmt.Sprintf("Document %d is corrupted in collection %s", id, file.File.Name))
		return
	}
	data = file.File.Buf[id+DOC_HEADER : id+DOC_HEADER+room]
	return
}

// Insert a document
func (file *DocFile) Insert(data []byte) (id uint64, err error) {
	len64 := uint64(len(data))
	if len64 > DOC_MAX_ROOM {
		err = errors.New("Document is too large")
		return
	}
	file.File.Ensure(len64)
	newAppend := file.File.Append
	file.File.Buf[id] = DOC_VALID
	newAppend += 1
	copy(file.File.Buf[newAppend:newAppend+len64], data)
	newAppend += len64
	copy(file.File.Buf[newAppend:newAppend+len64], make([]byte, len(data)))
	file.File.Append = newAppend
	return
}

// Replace a document by new content; or delete and re-insert the document if there is not enough room left
func (file *DocFile) Update(id uint64, data []byte) (newID uint64, err error) {
	if id < 0 || id > file.File.Append {
		err = errors.New(fmt.Sprintf("No such document (%d) in collection %s", id, file.File.Name))
		return
	}
	roomI64, ierr := binary.Varint(file.File.Buf[id+1 : id+4])
	room, len64 := uint64(roomI64), uint64(len(data))
	if ierr <= 0 || room < 0 || room > DOC_MAX_ROOM {
		err = errors.New(fmt.Sprintf("Document %d is corrupted in collection %s", id, file.File.Name))
		return
	}
	if len64 > DOC_MAX_ROOM {
		err = errors.New("Document is too large")
		return
	}
	if room >= len64 {
		copy(file.File.Buf[id+DOC_HEADER:id+DOC_HEADER+len64], data)
		copy(file.File.Buf[id+DOC_HEADER+len64:id+DOC_HEADER+len64*2], make([]byte, len(data)))
	}
	return
}

// Delete a document
func (file *DocFile) Delete(id int) (err error) {
	if file.File.Buf[id] == DOC_INVALID {
		return
	}
	if file.File.Buf[id] != DOC_VALID {
		err = errors.New(fmt.Sprintf("No such document (%d) in collection %s", id, file.File.Name))
		return
	}
	file.File.Buf[id] = 0
	return
}
