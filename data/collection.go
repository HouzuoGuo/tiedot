// Collection data file contains document data.
//
// Every document has a binary header and UTF-8 text content.
//
// Documents are inserted one after another, and occupies 2x original document
// size to leave room for future updates.
//
// Deleted documents are marked as deleted and the space is irrecoverable until
// a "scrub" action (in DB logic) is carried out.
//
// When update takes place, the new document may overwrite original document if
// there is enough space, otherwise the original document is marked as deleted
// and the updated document is inserted as a new document.

package data

import (
	"encoding/binary"

	"github.com/HouzuoGuo/tiedot/dberr"
)

// Collection file contains document headers and document text data.
type Collection struct {
	*DataFile
	*Config
}

// Open a collection file.
func (conf *Config) OpenCollection(path string) (col *Collection, err error) {
	col = new(Collection)
	col.DataFile, err = OpenDataFile(path, conf.ColFileGrowth)
	col.Config = conf
	col.Config.CalculateConfigConstants()
	return
}

// Find and retrieve a document by ID (physical document location). Return value is a copy of the document.
func (col *Collection) Read(id int) []byte {
	if id < 0 || id > col.Used-DocHeader || col.Buf[id] != 1 {
		return nil
	} else if room, _ := binary.Varint(col.Buf[id+1 : id+11]); room > int64(col.DocMaxRoom) {
		return nil
	} else if docEnd := id + DocHeader + int(room); docEnd >= col.Size {
		return nil
	} else {
		docCopy := make([]byte, room)
		copy(docCopy, col.Buf[id+DocHeader:docEnd])
		return docCopy
	}
}

// Insert a new document, return the new document ID.
func (col *Collection) Insert(data []byte) (id int, err error) {
	room := len(data) << 1
	if room > col.DocMaxRoom {
		return 0, dberr.New(dberr.ErrorDocTooLarge, col.DocMaxRoom, room)
	}
	id = col.Used
	docSize := DocHeader + room
	if err = col.EnsureSize(docSize); err != nil {
		return
	}
	col.Used += docSize
	// Write validity, room, document data and padding
	col.Buf[id] = 1
	binary.PutVarint(col.Buf[id+1:id+11], int64(room))
	copy(col.Buf[id+DocHeader:col.Used], data)
	for padding := id + DocHeader + len(data); padding < col.Used; padding += col.LenPadding {
		copySize := col.LenPadding
		if padding+col.LenPadding >= col.Used {
			copySize = col.Used - padding
		}
		copy(col.Buf[padding:padding+copySize], col.Padding)
	}
	return
}

// Overwrite or re-insert a document, return the new document ID if re-inserted.
func (col *Collection) Update(id int, data []byte) (newID int, err error) {
	dataLen := len(data)
	if dataLen > col.DocMaxRoom {
		return 0, dberr.New(dberr.ErrorDocTooLarge, col.DocMaxRoom, dataLen)
	}
	if id < 0 || id >= col.Used-DocHeader || col.Buf[id] != 1 {
		return 0, dberr.New(dberr.ErrorNoDoc, id)
	}
	currentDocRoom, _ := binary.Varint(col.Buf[id+1 : id+11])
	if currentDocRoom > int64(col.DocMaxRoom) {
		return 0, dberr.New(dberr.ErrorNoDoc, id)
	}
	if docEnd := id + DocHeader + int(currentDocRoom); docEnd >= col.Size {
		return 0, dberr.New(dberr.ErrorNoDoc, id)
	}
	if dataLen <= int(currentDocRoom) {
		padding := id + DocHeader + len(data)
		paddingEnd := id + DocHeader + int(currentDocRoom)
		// Overwrite data and then overwrite padding
		copy(col.Buf[id+DocHeader:padding], data)
		for ; padding < paddingEnd; padding += col.LenPadding {
			copySize := col.LenPadding
			if padding+col.LenPadding >= paddingEnd {
				copySize = paddingEnd - padding
			}
			copy(col.Buf[padding:padding+copySize], col.Padding)
		}
		return id, nil
	}

	// No enough room - re-insert the document
	col.Delete(id)
	return col.Insert(data)
}

// Delete a document by ID.
func (col *Collection) Delete(id int) error {

	if id < 0 || id > col.Used-DocHeader || col.Buf[id] != 1 {
		return dberr.New(dberr.ErrorNoDoc, id)
	}

	if col.Buf[id] == 1 {
		col.Buf[id] = 0
	}

	return nil
}

// Run the function on every document; stop when the function returns false.
func (col *Collection) ForEachDoc(fun func(id int, doc []byte) bool) {
	for id := 0; id < col.Used-DocHeader && id >= 0; {
		validity := col.Buf[id]
		room, _ := binary.Varint(col.Buf[id+1 : id+11])
		docEnd := id + DocHeader + int(room)
		if (validity == 0 || validity == 1) && room <= int64(col.DocMaxRoom) && docEnd > 0 && docEnd <= col.Used {
			if validity == 1 && !fun(id, col.Buf[id+DocHeader:docEnd]) {
				break
			}
			id = docEnd
		} else {
			// Corrupted document - move on
			id++
		}
	}
}
