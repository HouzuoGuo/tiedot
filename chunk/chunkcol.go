/* A tiedot collection is made of chunks, each chunk is independent fully featured collection. */
package chunk

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/HouzuoGuo/tiedot/chunkfile"
	"github.com/HouzuoGuo/tiedot/tdlog"
	"github.com/HouzuoGuo/tiedot/uid"
	"io"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const (
	// File name prefixes to identify the feature of file
	DAT_FILENAME_MAGIC       = "_data"
	UID_FILENAME_MAGIC       = "_uid"
	HASHTABLE_FILENAME_MAGIC = "ht_"

	UID_PATH       = "_uid"
	INDEX_PATH_SEP = "," // Separator between index path segments
)

type ChunkCol struct {
	BaseDir string             // File system directory path of the chunk
	Data    *chunkfile.ColFile // Collection document data file

	HTPaths    [][]string                      // Paths indexed by hash tables
	Hashtables []*chunkfile.HashTable          // Corresponds to HTPaths
	Path2HT    map[string]*chunkfile.HashTable // Joined index path to hash table mapping (including uid)
	UidHT      *chunkfile.HashTable            // The UID hash table
}

// Return string hash code using sdbm algorithm.
func StrHash(thing interface{}) uint64 {
	var hash rune
	for _, c := range fmt.Sprint(thing) {
		hash = c + (hash << 6) + (hash << 16) - hash
	}
	return uint64(hash)
}

// Resolve the attribute(s) in the document structure along the given path.
func GetIn(doc interface{}, path []string) (ret []interface{}) {
	docMap, ok := doc.(map[string]interface{})
	if !ok {
		tdlog.Printf("%v cannot be indexed because type conversation to map[string]interface{} failed", doc)
		return
	}
	var thing interface{} = docMap
	// Get into each path segment
	for i, seg := range path {
		if aMap, ok := thing.(map[string]interface{}); ok {
			thing = aMap[seg]
		} else if anArray, ok := thing.([]interface{}); ok {
			for _, element := range anArray {
				ret = append(ret, GetIn(element, path[i:])...)
			}
		} else {
			return nil
		}
	}
	switch thing.(type) {
	case []interface{}:
		return append(ret, thing.([]interface{})...)
	default:
		return append(ret, thing)
	}
}

// Open a chunk.
func OpenChunk(baseDir string) (chunk *ChunkCol, err error) {
	// Create the directory if it does not yet exist
	if err = os.MkdirAll(baseDir, 0700); err != nil {
		return
	}
	tdlog.Printf("Opening chunk %s", baseDir)
	chunk = &ChunkCol{BaseDir: baseDir, Path2HT: make(map[string]*chunkfile.HashTable)}
	// Open collection document data file
	tdlog.Printf("Opening collection data file %s", DAT_FILENAME_MAGIC)
	if chunk.Data, err = chunkfile.OpenCol(path.Join(baseDir, DAT_FILENAME_MAGIC)); err != nil {
		return
	}
	// Open UID hash table index
	tdlog.Printf("Opening UID hash table file %s", UID_FILENAME_MAGIC)
	if err = chunk.OpenIndex(UID_PATH, path.Join(baseDir, UID_FILENAME_MAGIC)); err != nil {
		return
	}
	chunk.UidHT = chunk.Hashtables[0] // so far, the UID hashtable is the only one opened

	// Walk the chunk's directory and look for other files to be opened
	walker := func(currPath string, info os.FileInfo, err2 error) error {
		if err2 != nil {
			// log and skip the error
			tdlog.Error(err)
			return nil
		}
		if info.IsDir() {
			return nil
		}
		switch {
		case strings.HasPrefix(info.Name(), DAT_FILENAME_MAGIC):
			return nil // Document data is already opened
		case strings.HasPrefix(info.Name(), UID_FILENAME_MAGIC):
			return nil // UID index is already opened
		case strings.HasPrefix(info.Name(), HASHTABLE_FILENAME_MAGIC):
			// Open a hash table index
			tdlog.Printf("Opening collection index hashtable %s", info.Name())
			chunk.OpenIndex(info.Name()[len(HASHTABLE_FILENAME_MAGIC):], info.Name())
		}
		return nil
	}
	err = filepath.Walk(baseDir, walker)
	return
}

// Open an index file.
func (col *ChunkCol) OpenIndex(joinedIndexPath string, filename string) (err error) {
	indexPath := strings.Split(joinedIndexPath, INDEX_PATH_SEP)
	hashtable, err := chunkfile.OpenHash(filename)
	if err != nil {
		return
	}
	col.Path2HT[joinedIndexPath] = hashtable
	col.HTPaths = append(col.HTPaths, indexPath)
	col.Hashtables = append(col.Hashtables, hashtable)
	return
}

// Create a new index. Do not operate on this collection until it finishes!
func (col *ChunkCol) Index(indexPath []string) error {
	joinedIndexPath := strings.Join(indexPath, ",")
	// Return error if the path is already indexed
	if _, alreadyExists := col.Path2HT[joinedIndexPath]; alreadyExists {
		return errors.New(fmt.Sprintf("Path %v is already indexed in collection %s", indexPath, col.BaseDir))
	}
	// Open new index
	newIndexFileName := HASHTABLE_FILENAME_MAGIC + joinedIndexPath
	col.OpenIndex(joinedIndexPath, path.Join(col.BaseDir, newIndexFileName))
	// Put all documents on the new index
	newIndex, ok := col.Path2HT[strings.Join(indexPath, ",")]
	if !ok {
		return errors.New(fmt.Sprintf("The new index %v is gone from %s??", indexPath, col.BaseDir))
	}
	col.ForAll(func(id uint64, doc interface{}) bool {
		for _, thing := range GetIn(doc, indexPath) {
			if thing != nil {
				newIndex.Put(StrHash(thing), id)
			}
		}
		return true
	})
	return nil
}

// Remove an index.
func (col *ChunkCol) Unindex(path []string) (err error) {
	joinedPath := strings.Join(path, ",")
	if joinedPath == UID_PATH {
		return errors.New("UID index may not be removed")
	}
	if _, found := col.Path2HT[joinedPath]; !found {
		return errors.New(fmt.Sprintf("Path %v is not indexed in collection %s", path, col.BaseDir))
	}
	// Find the index to be remove
	found := -1
	for i, paths := range col.HTPaths {
		if strings.Join(paths, INDEX_PATH_SEP) == joinedPath {
			found = i
			break
		}
	}
	if found == -1 {
		return errors.New(fmt.Sprintf("Path %v is not indexed in collection %s", path, col.BaseDir))
	}
	// Close the index file and delete it
	col.Hashtables[found].File.Close()
	if err = os.Remove(col.Hashtables[found].File.Name); err != nil {
		return
	}
	// Remove the index from other structures
	delete(col.Path2HT, joinedPath)
	col.Hashtables = append(col.Hashtables[0:found], col.Hashtables[found+1:]...)
	col.HTPaths = append(col.HTPaths[0:found], col.HTPaths[found+1:]...)
	return nil
}

// Put the document on all indexes.
func (col *ChunkCol) IndexDoc(id uint64, doc interface{}) {
	for i, path := range col.HTPaths {
		for _, toBeIndexed := range GetIn(doc, path) {
			if toBeIndexed != nil {
				col.Hashtables[i].Put(StrHash(toBeIndexed), id)
			}
		}
	}
}

// Remove the document from all indexes.
func (col *ChunkCol) UnindexDoc(id uint64, doc interface{}) {
	for i, path := range col.HTPaths {
		for _, toBeIndexed := range GetIn(doc, path) {
			if toBeIndexed != nil {
				col.Hashtables[i].Remove(StrHash(toBeIndexed), id)
			}
		}
	}
}

// Insert a document.
func (col *ChunkCol) Insert(doc interface{}) (id uint64, outOfSpace bool, err error) {
	data, err := json.Marshal(doc)
	if err != nil {
		return
	}
	if id, outOfSpace, err = col.Data.Insert(data); err != nil || outOfSpace {
		return
	}
	col.IndexDoc(id, doc)
	return
}

// Retrieve document by ID.
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

// Update a document, return its new ID.
func (col *ChunkCol) Update(id uint64, doc interface{}) (newID uint64, outOfSpace bool, err error) {
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
	var oldDoc interface{}
	if err = json.Unmarshal(oldData, &oldDoc); err == nil {
		col.UnindexDoc(id, oldDoc)
	} else {
		tdlog.Errorf("ERROR: The original document %d in %s is corrupted, this update will attempt to overwrite it", id, col.BaseDir)
	}
	// Update document data
	if newID, outOfSpace, err = col.Data.Update(id, data); err != nil {
		return
	}
	// Index updated document
	col.IndexDoc(newID, doc)
	return
}

// Delete a document by ID.
func (col *ChunkCol) Delete(id uint64) {
	var oldDoc interface{}
	err := col.Read(id, &oldDoc)
	if err != nil {
		return
	}
	col.Data.Delete(id)
	col.UnindexDoc(id, oldDoc)
}

// Insert a new document, and assign it a UID.
func (col *ChunkCol) InsertWithUID(doc interface{}) (newID uint64, newUID string, outOfSpace bool, err error) {
	newUID = uid.NextUID()
	if docMap, ok := doc.(map[string]interface{}); !ok {
		err = errors.New("Only JSON object document may have UID")
		return
	} else {
		docMap[UID_PATH] = newUID
		newID, outOfSpace, err = col.Insert(doc)
		return
	}
}

// Retrieve documentby UID, return its ID.
func (col *ChunkCol) ReadByUID(uid string, doc interface{}) (uint64, error) {
	var docID uint64
	found := false
	// Scan UID hash table, find potential matches
	col.UidHT.Get(StrHash(uid), 1, func(key, value uint64) bool {
		var candidate interface{}
		if col.Read(value, &candidate) == nil {
			if docMap, ok := candidate.(map[string]interface{}); ok {
				// Physically read the document to avoid hash collision
				if candidateUID, ok := docMap[UID_PATH]; ok {
					if stringUID, ok := candidateUID.(string); ok {
						if stringUID != uid {
							return false // A hash collision
						}
						docID = value
						found = true
					}
				}
			}
		}
		return true
	})
	if !found {
		return 0, errors.New(fmt.Sprintf("Document %s does not exist in %s", uid, col.BaseDir))
	}
	return docID, col.Read(docID, doc)
}

// Identify a document using UID and update it, return its new ID.
func (col *ChunkCol) UpdateByUID(uid string, doc interface{}) (newID uint64, outOfSpace bool, err error) {
	var throwAway interface{}
	if newID, err = col.ReadByUID(uid, &throwAway); err != nil {
		return
	} else {
		return col.Update(newID, doc)
	}
}

// Give a document (identified by ID) a new UID.
func (col *ChunkCol) ReassignUID(id uint64) (newID uint64, newUID string, newDoc interface{}, outOfSpace bool, err error) {
	newUID = uid.NextUID()
	var originalDoc interface{}
	if err = col.Read(id, &originalDoc); err != nil {
		return
	}
	if docWithUID, ok := originalDoc.(map[string]interface{}); !ok {
		err = errors.New("Only JSON object document may have UID")
		return
	} else {
		docWithUID[UID_PATH] = newUID
		newDoc = docWithUID
		newID, outOfSpace, err = col.Update(id, docWithUID)
		return
	}
}

// Delete a document by UID.
func (col *ChunkCol) DeleteByUID(uid string) {
	var throwAway interface{}
	if id, err := col.ReadByUID(uid, &throwAway); err == nil {
		col.Delete(id)
	}
}

// Deserialize each document and invoke the function on the deserialized docuemnt (Collection Scsn).
func (col *ChunkCol) ForAll(fun func(id uint64, doc interface{}) bool) {
	col.Data.ForAll(func(id uint64, data []byte) bool {
		var parsed interface{}
		if err := json.Unmarshal(data, &parsed); err != nil {
			tdlog.Errorf("Cannot parse document %d in %s to JSON", id, col.BaseDir)
			return true
		} else {
			return fun(id, parsed)
		}
	})
}

// Deserialize each document into template (pointer to an initialized struct), invoke the function on the deserialized document (Collection Scan).
func (col *ChunkCol) DeserializeAll(template interface{}, fun func(id uint64) bool) {
	col.Data.ForAll(func(id uint64, data []byte) bool {
		if err := json.Unmarshal(data, template); err != nil {
			return true
		} else {
			return fun(id)
		}
	})
}

// Compact the chunk and automatically repair any data/index damage.
func (col *ChunkCol) Scrub() {
	// Safety first - make a backup of existing data file
	bakFileName := DAT_FILENAME_MAGIC + "_" + strconv.Itoa(int(time.Now().UnixNano()))
	bakDest, err := os.Create(path.Join(col.BaseDir, bakFileName))
	if err != nil {
		tdlog.Errorf("Scrub: failed to backup existing data file, error %v", err)
	}
	defer bakDest.Close()
	if _, err := io.Copy(col.Data.File.Fh, bakDest); err != nil {
		tdlog.Errorf("Scrub: failed to backup existing data file, error %v", err)
	}
	// Read all documents into memory
	allDocs := make([]interface{}, 2048)
	col.ForAll(func(_ uint64, doc interface{}) bool {
		allDocs = append(allDocs, doc)
		return true
	})
	// Clear all indexes

}

// Flush collection data and index files.
func (col *ChunkCol) Flush() error {
	if err := col.Data.File.Flush(); err != nil {
		tdlog.Errorf("Failed to flush %s, reason: %v", col.Data.File.Name, err)
		return err
	}
	for _, ht := range col.Hashtables {
		if err := ht.File.Flush(); err != nil {
			tdlog.Errorf("Failed to flush %s, reason: %v", ht.File.Name, err)
			return err
		}
	}
	return nil
}

// Close the collection.
func (col *ChunkCol) Close() {
	if err := col.Data.File.Close(); err != nil {
		tdlog.Errorf("Failed to close %s, reason: %v", col.Data.File.Name, err)
	}
	for _, ht := range col.Hashtables {
		if err := ht.File.Close(); err != nil {
			tdlog.Errorf("Failed to close %s, reason: %v", ht.File.Name, err)
		}
	}
}
