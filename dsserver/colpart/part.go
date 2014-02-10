/* Independent collection partition. */
package colpart

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/HouzuoGuo/tiedot/dsserver/dstruct"
	"github.com/HouzuoGuo/tiedot/tdlog"
	"github.com/HouzuoGuo/tiedot/uid"
	"os"
	"path"
)

const (
	DAT_FILENAME_MAGIC = "_data" // Name of collection data file
	PK_FILENAME_MAGIC  = "_pk"   // Name of PK (primary index) file
)

type Partition struct {
	Number  int                // Number of the partition in collection
	BaseDir string             // File system directory path of the partition
	Data    *dstruct.ColFile   // Collection document data file
	PK      *dstruct.HashTable // PK hash table
}

// Return string hash code using sdbm algorithm.
func StrHash(thing interface{}) uint64 {
	var hash rune
	for _, c := range fmt.Sprint(thing) {
		hash = c + (hash << 6) + (hash << 16) - hash
	}
	return uint64(hash)
}

// Open a collection partition.
func OpenPart(number int, baseDir string) (part *Partition, err error) {
	// Create the directory if it does not yet exist
	if err = os.MkdirAll(baseDir, 0700); err != nil {
		return
	}
	tdlog.Printf("Opening partition %s", baseDir)
	part = &Partition{Number: number, BaseDir: baseDir}
	// Open collection document data file
	tdlog.Printf("Opening collection data file %s", DAT_FILENAME_MAGIC)
	if part.Data, err = dstruct.OpenCol(path.Join(baseDir, DAT_FILENAME_MAGIC)); err != nil {
		return
	}
	// Open PK hash table
	tdlog.Printf("Opening PK hash table file %s", PK_FILENAME_MAGIC)
	if part.PK, err = dstruct.OpenHash(path.Join(baseDir, PK_FILENAME_MAGIC), []string{uid.PK_NAME}); err != nil {
		return
	}
	return
}

// Insert a document.
func (col *Partition) Insert(doc map[string]interface{}) (physicalID uint64, err error) {
	docID, found := uid.PKOfDoc(doc)
	if !found {
		return 0, errors.New(fmt.Sprint("Document %v does not have ID", doc))
	}
	data, err := json.Marshal(doc)
	if err != nil {
		return
	}
	if physicalID, err = col.Data.Insert(data); err != nil {
		return
	}
	col.PK.Put(docID, physicalID)
	return
}

// Return the physical ID of document specified by primary key ID.
func (col *Partition) GetPhysicalID(id uint64) (physID uint64, err error) {
	// This function is called so often that we better inline the hash table key scan.
	var entry, bucket uint64 = 0, col.PK.HashKey(id)
	for {
		entryAddr := bucket*dstruct.BUCKET_SIZE + dstruct.BUCKET_HEADER_SIZE + entry*dstruct.ENTRY_SIZE
		entryKey, _ := binary.Uvarint(col.PK.File.Buf[entryAddr+1 : entryAddr+11])
		entryVal, _ := binary.Uvarint(col.PK.File.Buf[entryAddr+11 : entryAddr+21])
		if col.PK.File.Buf[entryAddr] == dstruct.ENTRY_VALID {
			if entryKey == id {
				var docMap map[string]interface{}
				if col.Read(entryVal, &docMap) == nil && err == nil {
					docID, docIDFound := uid.PKOfDoc(docMap)
					if docIDFound && docID == id {
						return entryVal, nil
					}
				}
			}
		} else if entryKey == 0 && entryVal == 0 {
			return 0, errors.New(fmt.Sprintf("Cannot find physical ID of %d", id))
		}
		if entry++; entry == dstruct.PER_BUCKET {
			entry = 0
			if bucket = col.PK.NextBucket(bucket); bucket == 0 {
				return 0, errors.New(fmt.Sprintf("Cannot find physical ID of %d", id))
			}
		}
	}
	return 0, errors.New(fmt.Sprintf("Cannot find physical ID of %s", id))
}

// Retrieve document by physical ID.
func (col *Partition) Read(id uint64, doc interface{}) error {
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
func (col *Partition) Update(id uint64, doc map[string]interface{}) (newID uint64, err error) {
	docID, found := uid.PKOfDoc(doc)
	if !found {
		return 0, errors.New(fmt.Sprintf("Document %v does not have ID", doc))
	}
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
		docID, uidFound := uid.PKOfDoc(oldDoc)
		if uidFound {
			col.PK.Remove(docID, id)
		}
	} else {
		tdlog.Errorf("ERROR: The original document %d in %s is corrupted, this update will attempt to overwrite it", id, col.BaseDir)
	}
	// Update document data
	if newID, err = col.Data.Update(id, data); err != nil {
		return
	}
	// Index updated document
	col.PK.Put(docID, newID)
	return
}

// Delete a document by physical ID.
func (col *Partition) Delete(id uint64) {
	var oldDoc map[string]interface{}
	err := col.Read(id, &oldDoc)
	if err != nil {
		return
	}
	col.Data.Delete(id)
	docID, found := uid.PKOfDoc(oldDoc)
	if found {
		col.PK.Remove(docID, id)
	}
}

// Deserialize each document and invoke the function on the deserialized document (Collection Scan).
func (col *Partition) ForAll(fun func(id uint64, doc map[string]interface{}) bool) {
	col.Data.ForAll(func(id uint64, data []byte) bool {
		var parsed map[string]interface{}
		if err := json.Unmarshal(data, &parsed); err != nil || parsed == nil {
			tdlog.Errorf("Cannot parse document %d in %s to JSON", id, col.BaseDir)
			return true
		} else {
			docID, found := uid.PKOfDoc(parsed)
			// Skip documents without valid PK
			if !found {
				return true
			}
			return fun(docID, parsed)
		}
	})
}

// Deserialize each document into template (pointer to an initialized struct), invoke the function on the deserialized document (Collection Scan).
func (col *Partition) DeserializeAll(template interface{}, fun func() bool) {
	col.Data.ForAll(func(_ uint64, data []byte) bool {
		if err := json.Unmarshal(data, template); err != nil {
			return true
		} else {
			return fun()
		}
	})
}

// Flush collection data and index files.
func (col *Partition) Flush() (err error) {
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
func (col *Partition) Close() {
	if err := col.Data.File.Close(); err != nil {
		tdlog.Errorf("Failed to close %s, reason: %v", col.Data.File.Name, err)
	}
	if err := col.PK.File.Close(); err != nil {
		tdlog.Errorf("Failed to close %s, reason: %v", col.PK.File.Name, err)
	}
}
