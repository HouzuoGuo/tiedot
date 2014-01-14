/* A tiedot collection is made of chunks, each chunk is independent fully featured collection. */
package chunk

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/HouzuoGuo/tiedot/chunkfile"
	"github.com/HouzuoGuo/tiedot/tdlog"
	"github.com/HouzuoGuo/tiedot/uid"
	"os"
	"path"
	"strconv"
)

const (
	DAT_FILENAME_MAGIC = "_data" // Name of collection data file
	PK_FILENAME_MAGIC  = "_pk"   // Name of PK (primary index) file
)

type ChunkCol struct {
	Number  int                  // Number of the chunk in collection
	BaseDir string               // File system directory path of the chunk
	Data    *chunkfile.ColFile   // Collection document data file
	PK      *chunkfile.HashTable // PK hash table
}

// Return string hash code using sdbm algorithm.
func StrHash(thing interface{}) uint64 {
	var hash rune
	for _, c := range fmt.Sprint(thing) {
		hash = c + (hash << 6) + (hash << 16) - hash
	}
	return uint64(hash)
}

// Open a chunk.
func OpenChunk(number int, baseDir string) (chunk *ChunkCol, err error) {
	// Create the directory if it does not yet exist
	if err = os.MkdirAll(baseDir, 0700); err != nil {
		return
	}
	tdlog.Printf("Opening chunk %s", baseDir)
	chunk = &ChunkCol{Number: number, BaseDir: baseDir}
	// Open collection document data file
	tdlog.Printf("Opening collection data file %s", DAT_FILENAME_MAGIC)
	if chunk.Data, err = chunkfile.OpenCol(path.Join(baseDir, DAT_FILENAME_MAGIC)); err != nil {
		return
	}
	// Open PK hash table
	tdlog.Printf("Opening PK hash table file %s", PK_FILENAME_MAGIC)
	if chunk.PK, err = chunkfile.OpenHash(path.Join(baseDir, PK_FILENAME_MAGIC), []string{uid.PK_NAME}); err != nil {
		return
	}
	return
}

// Insert a document.
func (col *ChunkCol) Insert(doc map[string]interface{}) (id uint64, err error) {
	data, err := json.Marshal(doc)
	if err != nil {
		return
	}
	if id, err = col.Data.Insert(data); err != nil {
		return
	}
	// Put string represented integer PK and the document ID on index
	col.PK.Put(uint64(uid.PKOfDoc(doc, true)), id)
	return
}

// Return the physical ID of document specified by primary key ID.
func (col *ChunkCol) GetPhysicalID(id int) (physID uint64, err error) {
	// This function is called so often that we better inline the hash table key scan.
	var entry, bucket uint64 = 0, col.PK.HashKey(uint64(id))
	for {
		entryAddr := bucket*chunkfile.BUCKET_SIZE + chunkfile.BUCKET_HEADER_SIZE + entry*chunkfile.ENTRY_SIZE
		entryKey, _ := binary.Uvarint(col.PK.File.Buf[entryAddr+1 : entryAddr+11])
		entryVal, _ := binary.Uvarint(col.PK.File.Buf[entryAddr+11 : entryAddr+21])
		if col.PK.File.Buf[entryAddr] == chunkfile.ENTRY_VALID {
			if int(entryKey) == id {
				var docMap map[string]interface{}
				if col.Read(entryVal, &docMap) == nil {
					strint, err := strconv.Atoi(docMap[uid.PK_NAME].(string))
					if err == nil && strint == id {
						return entryVal, nil
					}
				}
			}
		} else if entryKey == 0 && entryVal == 0 {
			return 0, errors.New(fmt.Sprintf("Cannot find physical ID of %d", id))
		}
		if entry++; entry == chunkfile.PER_BUCKET {
			entry = 0
			if bucket = col.PK.NextBucket(bucket); bucket == 0 {
				return 0, errors.New(fmt.Sprintf("Cannot find physical ID of %d", id))
			}
		}
	}
	return 0, errors.New(fmt.Sprintf("Cannot find physical ID of %s", id))
}

// Retrieve document by physical ID.
func (col *ChunkCol) Read(id uint64, doc interface{}) error {
	data := col.Data.Read(id)
	if data == nil {
		return errors.New(fmt.Sprintf("Document %d does not exist in %s", id, col.BaseDir))
	}
	if err := json.Unmarshal(data, &doc); err != nil {
		msg := fmt.Sprintf("Cannot parse document %d in %s to JSON", id, col.BaseDir)
		tdlog.Println(msg)
		return errors.New(msg)
	}
	return nil
}

// Update a document by physical ID, return its new physical ID.
func (col *ChunkCol) Update(id uint64, doc map[string]interface{}) (newID uint64, err error) {
	data, err := json.Marshal(doc)
	if err != nil {
		return
	}
	// Read the original document
	oldData := col.Data.Read(id)
	if oldData == nil {
		err = errors.New(fmt.Sprintf("Document %d does not exist in %s", id, col.BaseDir))
		return
	}
	// Remove the original document from indexes
	var oldDoc map[string]interface{}
	if err = json.Unmarshal(oldData, &oldDoc); err == nil {
		col.PK.Remove(uint64(uid.PKOfDoc(oldDoc, false)), id)
	} else {
		tdlog.Errorf("ERROR: The original document %d in %s is corrupted, this update will attempt to overwrite it", id, col.BaseDir)
	}
	// Update document data
	if newID, err = col.Data.Update(id, data); err != nil {
		return
	}
	// Index updated document
	col.PK.Put(uint64(uid.PKOfDoc(doc, true)), newID)
	return
}

// Delete a document by physical ID.
func (col *ChunkCol) Delete(id uint64) {
	var oldDoc map[string]interface{}
	err := col.Read(id, &oldDoc)
	if err != nil {
		return
	}
	col.Data.Delete(id)
	col.PK.Remove(uint64(uid.PKOfDoc(oldDoc, false)), id)
}

// Deserialize each document and invoke the function on the deserialized document (Collection Scan).
func (col *ChunkCol) ForAll(fun func(id int, doc map[string]interface{}) bool) {
	col.Data.ForAll(func(id uint64, data []byte) bool {
		var parsed map[string]interface{}
		if err := json.Unmarshal(data, &parsed); err != nil || parsed == nil {
			tdlog.Errorf("Cannot parse document %d in %s to JSON", id, col.BaseDir)
			return true
		} else {
			persistID := uid.PKOfDoc(parsed, false)
			// Skip documents without valid PK
			if persistID < 0 {
				return true
			}
			return fun(persistID, parsed)
		}
	})
}

// Deserialize each document into template (pointer to an initialized struct), invoke the function on the deserialized document (Collection Scan).
func (col *ChunkCol) DeserializeAll(template interface{}, fun func() bool) {
	col.Data.ForAll(func(_ uint64, data []byte) bool {
		if err := json.Unmarshal(data, template); err != nil {
			return true
		} else {
			return fun()
		}
	})
}

// Flush collection data and index files.
func (col *ChunkCol) Flush() (err error) {
	if err = col.Data.File.Flush(); err != nil {
		tdlog.Errorf("Failed to flush %s, reason: %v", col.Data.File.Name, err)
		return
	}
	if err = col.PK.File.Flush(); err != nil {
		tdlog.Errorf("Failed to flush %s, reason: %v", col.PK.File.Name, err)
		return
	}
	return
}

// Close the collection.
func (col *ChunkCol) Close() {
	if err := col.Data.File.Close(); err != nil {
		tdlog.Errorf("Failed to close %s, reason: %v", col.Data.File.Name, err)
	}
	if err := col.PK.File.Close(); err != nil {
		tdlog.Errorf("Failed to close %s, reason: %v", col.PK.File.Name, err)
	}
}
