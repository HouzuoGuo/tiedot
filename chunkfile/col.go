/* Document collection file. */
package chunkfile

import (
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/HouzuoGuo/tiedot/file"
	"github.com/HouzuoGuo/tiedot/tdlog"
)

const (
	COL_FILE_SIZE = uint64(1024 * 1024 * 16) // Size of collection data file
	DOC_MAX_ROOM  = uint64(1024 * 1024 * 16) // Max single document size
	DOC_HEADER    = 1 + 10                   // Size of document header - validity (byte), document room (uint64)
	DOC_VALID     = byte(1)                  // Document valid flag
	DOC_INVALID   = byte(0)                  // Document invalid flag

	// Pre-compiled document padding (2048 spaces)
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
	File *file.File
}

// Open a collection file.
func OpenCol(name string) (*ColFile, error) {
	file, err := file.Open(name, COL_FILE_SIZE)
	return &ColFile{File: file}, err
}

// Retrieve document data given its ID.
func (col *ColFile) Read(id uint64) []byte {
	if col.File.UsedSize < DOC_HEADER || id >= col.File.UsedSize-DOC_HEADER {
		return nil
	}
	if col.File.Buf[id] != DOC_VALID {
		return nil
	}
	if room, _ := binary.Uvarint(col.File.Buf[id+1 : id+11]); room > DOC_MAX_ROOM {
		return nil
	} else {
		docCopy := make([]byte, room)
		docEnd := id + DOC_HEADER + room
		if docEnd >= col.File.Size {
			return nil
		}
		copy(docCopy, col.File.Buf[id+DOC_HEADER:docEnd])
		return docCopy
	}
}

// Insert a document, return its ID.
func (col *ColFile) Insert(data []byte) (id uint64, outOfSpace bool, err error) {
	len64 := uint64(len(data))
	room := len64 + len64
	if room > DOC_MAX_ROOM {
		err = errors.New(fmt.Sprintf("Document is too large"))
		return
	}
	// Keep track of new document ID and used space
	id = col.File.UsedSize
	if !col.File.CheckSize(DOC_HEADER + room) {
		outOfSpace = true
		return
	}
	col.File.UsedSize = id + DOC_HEADER + room
	// Make document header, then copy document data
	col.File.Buf[id] = 1
	binary.PutUvarint(col.File.Buf[id+1:id+DOC_HEADER], room)
	paddingBegin := id + DOC_HEADER + len64
	copy(col.File.Buf[id+DOC_HEADER:paddingBegin], data)
	// Fill up padding space
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
	return
}

// Update a document, return its new ID.
func (col *ColFile) Update(id uint64, data []byte) (newID uint64, outOfSpace bool, err error) {
	len64 := uint64(len(data))
	if len64 > DOC_MAX_ROOM {
		err = errors.New(fmt.Sprintf("Updated document is too large"))
		return
	}
	if col.File.UsedSize < DOC_HEADER || id >= col.File.UsedSize-DOC_HEADER {
		err = errors.New(fmt.Sprintf("Document %d does not exist in %s", id, col.File.Name))
		return
	}
	if col.File.Buf[id] != DOC_VALID {
		err = errors.New(fmt.Sprintf("Document %d does not exist in %s", id, col.File.Name))
		return
	}
	if room, _ := binary.Uvarint(col.File.Buf[id+1 : id+11]); room > DOC_MAX_ROOM || id+room >= col.File.UsedSize {
		err = errors.New(fmt.Sprintf("Document %d does not exist in %s", id, col.File.Name))
		return
	} else {
		if len64 <= room {
			// There is enough room for the updated document
			// Overwrite document data
			paddingBegin := id + DOC_HEADER + len64
			copy(col.File.Buf[id+DOC_HEADER:paddingBegin], data)
			// Overwrite padding space
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
			return id, false, nil
		}
		// There is not enough room for updated content, so delete the original document and re-insert
		col.Delete(id)
		return col.Insert(data)
	}
}

// Delete a document.
func (col *ColFile) Delete(id uint64) {
	if col.File.UsedSize < DOC_HEADER || id >= col.File.UsedSize-DOC_HEADER {
		return
	}
	if col.File.Buf[id] == DOC_VALID {
		col.File.Buf[id] = DOC_INVALID
	}
}

// Scan the entire data file, look for documents and invoke the function on each.
func (col *ColFile) ForAll(fun func(id uint64, doc []byte) bool) {
	addr := uint64(0)
	for {
		if col.File.UsedSize < DOC_HEADER || addr >= col.File.UsedSize-DOC_HEADER {
			break
		}
		// Read document header - validity and room
		validity := col.File.Buf[addr]
		room, _ := binary.Uvarint(col.File.Buf[addr+1 : addr+11])
		if validity != DOC_VALID && validity != DOC_INVALID || room > DOC_MAX_ROOM {
			// If the document does not contain valid header, skip it
			tdlog.Errorf("ERROR: The document at %d in %s is corrupted", addr, col.File.Name)
			// Move forward until we meet a valid document header
			for addr++; col.File.Buf[addr] != DOC_VALID && col.File.Buf[addr] != DOC_INVALID && addr < col.File.UsedSize-DOC_HEADER; addr++ {
			}
			continue
		}
		// If the function returns false, do not continue scanning
		if validity == DOC_VALID && !fun(addr, col.File.Buf[addr+DOC_HEADER:addr+DOC_HEADER+room]) {
			break
		}
		addr += DOC_HEADER + room
	}
}
