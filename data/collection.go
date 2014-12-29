/*
Data structure - document collection.
Collection data file contains document data - a combination of binary headers and readable text.
New documents are inserted one after another; each document has a binary header and text content encoded in UTF-8 which
is native to Go. When inserted, a new document initially occupies 2 times original size to leave room for future growth.
Deleted documents are marked as deleted in their header; the space occupied by deleted documents is irrecoverable until
the next "scrub" action (implemented in DB logic).
When document is updated, the new document may overwrite the original if there is enough space; otherwise the original
is marked as deleted, and the new one is inserted as if new, with space left for future growth.
Physical locations are used to locate documents, boundary checking mechanism eliminates invalid location access.
*/
package data

import (
	"encoding/binary"
	"github.com/HouzuoGuo/tiedot/dberr"
)

const (
	// Collection file initial size & size growth (16 MBytes)
	COL_FILE_GROWTH = 16 * 1048576
	// Max document size (2 MBytes)
	DOC_MAX_ROOM = 2 * 1048576
	// Document header size (validity - byte, document room - uint64)
	DOC_HEADER = 1 + 8
	// Pre-compiled document padding (128 spaces)
	PADDING     = "                                                                                                                                "
	LEN_PADDING = uint64(len(PADDING))
)

// Collection file contains document binary header and text data.
type Collection struct {
	*DataFile
}

// Open/create a collection file.
func OpenCollection(path string) (col *Collection, err error) {
	col = new(Collection)
	col.DataFile, err = OpenDataFile(path, COL_FILE_GROWTH)
	return
}

// Retrieve a document by its physical location. Return value is a copy of the document (not a buffer slice).
func (col *Collection) Read(loc uint64) []byte {
	if loc > col.Used-DOC_HEADER || col.Buf[loc] != 1 {
		return nil
	} else if room := binary.LittleEndian.Uint64(col.Buf[loc+1:]); room > DOC_MAX_ROOM {
		return nil
	} else if docEnd := loc + DOC_HEADER + room; docEnd >= col.Size {
		return nil
	} else {
		docCopy := make([]byte, room)
		copy(docCopy, col.Buf[loc+DOC_HEADER:])
		return docCopy
	}
}

// Insert a new document, return the new document's physical location.
func (col *Collection) Insert(data []byte) (loc uint64, err error) {
	room := uint64(len(data) << 1)
	if room > DOC_MAX_ROOM {
		return 0, dberr.Make(dberr.ErrorDocTooLarge, DOC_MAX_ROOM, room)
	}
	loc = col.Used
	docSize := DOC_HEADER + room
	if err = col.EnsureSize(docSize); err != nil {
		return
	}
	col.Used += docSize
	// Header - valid (1), room
	col.Buf[loc] = 1
	binary.LittleEndian.PutUint64(col.Buf[loc+1:], room)
	// Document text data and padding
	copy(col.Buf[loc+DOC_HEADER:col.Used], data)
	for padding := loc + DOC_HEADER + uint64(len(data)); padding < col.Used; padding += LEN_PADDING {
		copySize := LEN_PADDING
		if padding+LEN_PADDING >= col.Used {
			copySize = col.Used - padding
		}
		copy(col.Buf[padding:padding+copySize], PADDING)
	}
	return
}

// Update a document. Return identical document location if document was overwritten; otherwise return its new location.
func (col *Collection) Update(loc uint64, data []byte) (maybeNewLoc uint64, err error) {
	if newSize := uint64(len(data)); newSize > DOC_MAX_ROOM {
		return 0, dberr.Make(dberr.ErrorDocTooLarge, DOC_MAX_ROOM, newSize)
	} else if loc < 0 || loc >= col.Used-DOC_HEADER || col.Buf[loc] != 1 {
		return 0, dberr.Make(dberr.ErrorNoDoc, loc)
	} else if room := binary.LittleEndian.Uint64(col.Buf[loc+1:]); room > DOC_MAX_ROOM {
		return 0, dberr.Make(dberr.ErrorNoDoc, loc)
	} else if docEnd := loc + DOC_HEADER + room; docEnd >= col.Size {
		return 0, dberr.Make(dberr.ErrorNoDoc, loc)
	} else if newSize <= room {
		// There is enough room left, re-calculate padding size.
		padding := loc + DOC_HEADER + newSize
		paddingEnd := loc + DOC_HEADER + room
		// Overwrite data and then overwrite padding
		copy(col.Buf[loc+DOC_HEADER:padding], data)
		for ; padding < paddingEnd; padding += LEN_PADDING {
			copySize := LEN_PADDING
			if padding+LEN_PADDING >= paddingEnd {
				copySize = paddingEnd - padding
			}
			copy(col.Buf[padding:padding+copySize], PADDING)
		}
		return loc, nil
	} else {
		// There is not enough room for the updated document, so re-insert it.
		col.Delete(loc)
		return col.Insert(data)
	}
}

// Mark a document as deleted. Its space is wasted until the next "scrub" operation (implemented in DB logic).
func (col *Collection) Delete(loc uint64) (err error) {
	if loc < 0 || loc > col.Used-DOC_HEADER || col.Buf[loc] != 1 {
		return dberr.Make(dberr.ErrorNoDoc, loc)
	}
	if col.Buf[loc] == 1 {
		col.Buf[loc] = 0
	}
	return nil
}

// Run fun on all documents, stop when fun returns false. Silently ignore all potential document corruption.
func (col *Collection) ForEachDoc(fun func(loc uint64, doc []byte) bool) {
	for loc := uint64(0); loc < col.Used-DOC_HEADER; {
		validity := col.Buf[loc]
		room := binary.LittleEndian.Uint64(col.Buf[loc+1:])
		docEnd := loc + DOC_HEADER + room
		if (validity == 0 || validity == 1) && room <= DOC_MAX_ROOM && docEnd > 0 && docEnd <= col.Used {
			if validity == 1 && !fun(loc, col.Buf[loc+DOC_HEADER:docEnd]) {
				break
			}
			loc = docEnd
		} else {
			// Corrupted document - move on
			loc++
		}
	}
}
