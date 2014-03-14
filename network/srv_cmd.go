/* Server command implementations. */
package network

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/HouzuoGuo/tiedot/colpart"
	"github.com/HouzuoGuo/tiedot/dstruct"
	"github.com/HouzuoGuo/tiedot/tdlog"
	"github.com/HouzuoGuo/tiedot/uid"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
)

const (
	NUMCHUNKS_FILENAME      = "numchunks"
	HASHTABLE_DIRNAME_MAGIC = "ht_"    // Hash table directory name prefix
	CHUNK_DIRNAME_MAGIC     = "chunk_" // Chunk directory name prefix
	INDEX_PATH_SEP          = ","      // Separator between index path segments
)

// Reload collection configurations.
func (server *Server) Reload(_ []string) (err interface{}) {
	server.SchemaUpdateInProgress = true
	// Save whatever I already have, and get rid of everything
	server.FlushAll(nil)
	server.ColNumParts = make(map[string]int)
	server.ColIndexPath = make(map[string][][]string)
	server.ColIndexPathStr = make(map[string][]string)
	server.ColParts = make(map[string]*colpart.Partition)
	server.Htables = make(map[string]map[string]*dstruct.HashTable)
	// Read the DB directory
	files, err := ioutil.ReadDir(server.DBDir)
	if err != nil {
		return
	}
	for _, f := range files {
		// Sub-directories are collections
		if f.IsDir() {
			// Read the "numchunks" file - its should contain a positive integer in the content
			var numchunksFH *os.File
			colName := f.Name()
			numchunksFH, err = os.OpenFile(path.Join(server.DBDir, colName, NUMCHUNKS_FILENAME), os.O_CREATE|os.O_RDWR, 0600)
			defer numchunksFH.Close()
			if err != nil {
				return
			}
			numchunksContent, err := ioutil.ReadAll(numchunksFH)
			if err != nil {
				panic(err)
			}
			numchunks, err := strconv.Atoi(string(numchunksContent))
			if err != nil || numchunks < 1 {
				tdlog.Panicf("Rank %d: Cannot figure out number of chunks for collection %s, manually repair it maybe? %v", server.Rank, server.DBDir, err)
			}
			server.ColNumParts[colName] = numchunks
			server.ColIndexPath[colName] = make([][]string, 0, 0)
			server.ColIndexPathStr[colName] = make([]string, 0, 0)
			// Abort the program if total number of processes is not enough for a collection
			if server.TotalRank < numchunks {
				panic(fmt.Sprintf("Please start at least %d processes, because collection %s has %d partitions", numchunks, colName, numchunks))
			}
			colDir := path.Join(server.DBDir, colName)
			if server.Rank < numchunks {
				tdlog.Printf("Rank %d: I am going to open my partition in %s", server.Rank, f.Name())
				// Open data partition
				part, err := colpart.OpenPart(path.Join(colDir, CHUNK_DIRNAME_MAGIC+strconv.Itoa(server.Rank)))
				if err != nil {
					return err
				}
				// Put the partition into server structure
				server.ColParts[colName] = part
				server.Htables[colName] = make(map[string]*dstruct.HashTable)
			}
			// Look for indexes in the collection
			walker := func(_ string, info os.FileInfo, err2 error) error {
				if err2 != nil {
					tdlog.Error(err)
					return nil
				}
				if info.IsDir() {
					switch {
					case strings.HasPrefix(info.Name(), HASHTABLE_DIRNAME_MAGIC):
						// Figure out indexed path - including the partition number
						indexPathStr := info.Name()[len(HASHTABLE_DIRNAME_MAGIC):]
						indexPath := strings.Split(indexPathStr, INDEX_PATH_SEP)
						// Put the schema into server structures
						server.ColIndexPathStr[colName] = append(server.ColIndexPathStr[colName], indexPathStr)
						server.ColIndexPath[colName] = append(server.ColIndexPath[colName], indexPath)
						if server.Rank < numchunks {
							tdlog.Printf("Rank %d: I am going to open my partition in hashtable %s", server.Rank, info.Name())
							ht, err := dstruct.OpenHash(path.Join(colDir, info.Name(), strconv.Itoa(server.Rank)), indexPath)
							if err != nil {
								return err
							}
							server.Htables[colName][indexPathStr] = ht
						}
					}
				}
				return nil
			}
			err = filepath.Walk(colDir, walker)
		}
	}
	server.SchemaUpdateInProgress = false
	return nil
}

// Call flush on all mapped files.
func (srv *Server) FlushAll(_ []string) (_ interface{}) {
	for _, part := range srv.ColParts {
		part.Flush()
	}
	for _, htMap := range srv.Htables {
		for _, ht := range htMap {
			ht.File.Flush()
		}
	}
	return nil
}

// Create a collection.
func (srv *Server) ColCreate(params []string) (err interface{}) {
	colName := params[1]
	numParts := params[2]
	// Check input parameters
	numPartsI, err := strconv.Atoi(numParts)
	if err != nil {
		return
	}
	if numPartsI > srv.TotalRank {
		return errors.New(fmt.Sprintf("(ColCreate %s) There are not enough processes running", colName))
	}
	// Make new files and directories for the collection
	if err = os.MkdirAll(path.Join(srv.DBDir, colName), 0700); err != nil {
		return
	}
	if err = ioutil.WriteFile(path.Join(srv.DBDir, colName, NUMCHUNKS_FILENAME), []byte(numParts), 0600); err != nil {
		return
	}
	// Reload my config
	if err = srv.Reload(nil); err != nil {
		return
	}
	// Inform other ranks to reload their config
	if !srv.BroadcastAway(RELOAD, true, false) {
		return errors.New(fmt.Sprintf("(ColCreate %s) Failed to reload configuration", colName))
	}
	return nil
}

// Return all collection name VS number of partitions in JSON.
func (srv *Server) ColAll(_ []string) (neverErr interface{}) {
	return srv.ColNumParts
}

// Rename a collection.
func (srv *Server) ColRename(params []string) (err interface{}) {
	oldName := params[1]
	newName := params[2]
	// Check input names
	if oldName == newName {
		return errors.New(fmt.Sprintf("(ColRename %s %s) New name may not be the same as old name", oldName, newName))
	}
	if _, alreadyExists := srv.ColNumParts[newName]; alreadyExists {
		return errors.New(fmt.Sprintf("(ColRename %s %s) New name is already used", oldName, newName))
	}
	if _, exists := srv.ColNumParts[oldName]; !exists {
		return errors.New(fmt.Sprintf("(ColRename %s %s) Old name does not exist", oldName, newName))
	}
	// Rename collection directory
	if err = os.Rename(path.Join(srv.DBDir, oldName), path.Join(srv.DBDir, newName)); err != nil {
		return
	}
	// Reload myself and inform other ranks to reload their config
	srv.Reload(nil)
	if !srv.BroadcastAway(RELOAD, true, false) {
		return errors.New(fmt.Sprintf("(ColRename %s %s) Failed to reload configuration", oldName, newName))
	}
	return nil
}

// Drop a collection.
func (srv *Server) ColDrop(params []string) (err interface{}) {
	colName := params[1]
	// Check input name
	if _, exists := srv.ColNumParts[colName]; !exists {
		return errors.New(fmt.Sprintf("(ColDrop %s) Collection does not exist", colName))
	}
	// Remove the collection from file system
	if err = os.RemoveAll(path.Join(srv.DBDir, colName)); err != nil {
		return
	}
	// Reload myself and inform other ranks to reload their config
	srv.Reload(nil)
	if !srv.BroadcastAway(RELOAD, true, false) {
		return errors.New(fmt.Sprintf("(ColDrop %s) Failed to reload configuration", colName))
	}
	return nil
}

// Ping, Ping1, PingJS are for testing purpose, they do not manipulate any data.
func (srv *Server) Ping(_ []string) (strNoError interface{}) {
	return ACK
}
func (srv *Server) Ping1(_ []string) (uint64NoError interface{}) {
	return uint64(1)
}
func (srv *Server) PingJS(_ []string) (jsonNoError interface{}) {
	return []string{ACK, ACK}
}
func (srv *Server) PingErr(_ []string) (mustBErr interface{}) {
	return errors.New("this is an error")
}

// Insert a document into my partition of the collection.
func (srv *Server) DocInsert(params []string) (strOrErr interface{}) {
	colName := params[1]
	jsonDoc := params[2]
	// Check input collection name and JSON document string
	if col, exists := srv.ColParts[colName]; !exists {
		return errors.New(fmt.Sprintf("(DocInsert %s) My rank does not own a partition of the collection", colName))
	} else {
		var doc map[string]interface{}
		if strOrErr = json.Unmarshal([]byte(jsonDoc), &doc); strOrErr != nil {
			return errors.New(fmt.Sprintf("(DocInsert %s) Input JSON is malformed", colName))
		}
		// Insert the document into my partition
		var newDocID uint64
		if newDocID, strOrErr = col.Insert(doc); strOrErr != nil {
			return errors.New(fmt.Sprintf("(DocInsert %s) %v", colName, strOrErr))
		}
		return strconv.FormatUint(newDocID, 10)
	}
}

// Get a document from my partition of the collection.
func (srv *Server) DocGet(params []string) (strOrErr interface{}) {
	colName := params[1]
	id := params[2]
	// Check input collection name and ID
	idInt, strOrErr := strconv.ParseUint(id, 10, 64)
	if strOrErr != nil {
		return errors.New(fmt.Sprintf("(DocGet %s) %s is not a valid document ID", colName, id))
	}
	if col, exists := srv.ColParts[colName]; !exists {
		return errors.New(fmt.Sprintf("(DocGet %s) My rank does not own a partition of the collection", colName))
	} else {
		// Read document from partition and return
		if jsonStr, err := col.ReadStr(idInt); err != nil {
			return err
		} else {
			return jsonStr
		}
	}
}

// Update a document in my partition.
func (srv *Server) DocUpdate(params []string) (strOrErr interface{}) {
	colName := params[1]
	id := params[2]
	jsonDoc := params[3]
	// Check input collection name, new document JSON, and UID
	idInt, strOrErr := strconv.ParseUint(id, 10, 64)
	if strOrErr != nil {
		return errors.New(fmt.Sprintf("(DocUpdate %s) %s is not a valid document ID", colName, id))
	}
	if col, exists := srv.ColParts[colName]; !exists {
		return errors.New(fmt.Sprintf("(DocUpdate %s) My rank does not own a partition of the collection", colName))
	} else {
		var doc map[string]interface{}
		if strOrErr = json.Unmarshal([]byte(jsonDoc), &doc); strOrErr != nil {
			return errors.New(fmt.Sprintf("(DocUpdate %s) Input JSON is malformed", colName))
		}
		doc[uid.PK_NAME] = id // client is not supposed to change UID, just to make sure
		var newDocID uint64
		if newDocID, strOrErr = col.Update(idInt, doc); strOrErr != nil {
			return
		}
		return strconv.FormatUint(newDocID, 10)
	}
}

// Update a document in my partition.
func (srv *Server) DocDelete(params []string) (err interface{}) {
	colName := params[1]
	id := params[2]
	// Check input collection name, new document JSON, and UID
	idInt, err := strconv.ParseUint(id, 10, 64)
	if err != nil {
		return errors.New(fmt.Sprintf("(DocDelete %s) %s is not a valid document ID", colName, id))
	}
	if col, exists := srv.ColParts[colName]; !exists {
		return errors.New(fmt.Sprintf("(DocDelete %s) My rank does not own a partition of the collection", colName))
	} else {
		col.Delete(idInt)
	}
	return nil
}

// Put a key-value pair into hash table.
func (srv *Server) HTPut(params []string) (err interface{}) {
	colName := params[1]
	htName := params[2]
	key := params[3]
	val := params[4]
	if col, exists := srv.Htables[colName]; !exists {
		return errors.New(fmt.Sprintf("(HTPut %s) My rank %d does not own a partition of the hash table", colName, srv.Rank))
	} else {
		if ht, exists := col[htName]; !exists {
			return errors.New(fmt.Sprintf("(HTPut %s) Hash table %s does not exist", colName, htName))
		} else {
			var keyInt, valInt uint64
			keyInt, err = strconv.ParseUint(key, 10, 64)
			if err != nil {
				return
			}
			valInt, err = strconv.ParseUint(val, 10, 64)
			if err != nil {
				return
			}
			ht.Put(keyInt, valInt)
		}
	}
	return nil
}

// Get a key's associated values.
func (srv *Server) HTGet(params []string) (strOrErr interface{}) {
	colName := params[1]
	htName := params[2]
	key := params[3]
	limit := params[4]
	if col, exists := srv.Htables[colName]; !exists {
		return errors.New(fmt.Sprintf("(HTGet %s) My rank does not own a partition of the hash table", colName))
	} else {
		if ht, exists := col[htName]; !exists {
			return errors.New(fmt.Sprintf("(HTGet %s) Hash table %s does not exist", colName, htName))
		} else {
			var keyInt, limitInt uint64
			if keyInt, strOrErr = strconv.ParseUint(key, 10, 64); strOrErr != nil {
				return
			}
			if limitInt, strOrErr = strconv.ParseUint(limit, 10, 64); strOrErr != nil {
				return
			}
			vals := ht.Get(keyInt, limitInt)
			resp := make([]string, len(vals))
			for i, val := range vals {
				resp[i] = strconv.FormatUint(val, 10)
			}
			return strings.Join(resp, " ")
		}
	}
	return nil
}

// Remove a key-value pair.
func (srv *Server) HTDelete(params []string) (err interface{}) {
	colName := params[1]
	htName := params[2]
	key := params[3]
	val := params[4]
	if col, exists := srv.Htables[colName]; !exists {
		return errors.New(fmt.Sprintf("(HTDelete %s) My rank does not own a partition of the hash table", colName))
	} else {
		if ht, exists := col[htName]; !exists {
			return errors.New(fmt.Sprintf("(HTDelete %s) Hash table %s does not exist", colName, htName))
		} else {
			var keyInt, valInt uint64
			if keyInt, err = strconv.ParseUint(key, 10, 64); err != nil {
				return
			}
			if valInt, err = strconv.ParseUint(val, 10, 64); err != nil {
				return
			}
			ht.Remove(keyInt, valInt)
		}
	}
	return nil
}

// Create an index.
func (srv *Server) IdxCreate(params []string) (err interface{}) {
	colName := params[1]
	idxPath := params[2]
	// Verify that the collection exists
	if _, exists := srv.ColNumParts[colName]; !exists {
		return errors.New(fmt.Sprintf("(IdxCreate %s) Collection does not exist", colName))
	}
	// Create hash table directory
	if err = os.MkdirAll(path.Join(srv.DBDir, colName, HASHTABLE_DIRNAME_MAGIC+idxPath), 0700); err != nil {
		return
	}
	// Reload my config
	if err = srv.Reload(nil); err != nil {
		return
	}
	// Inform other ranks to reload their config
	if !srv.BroadcastAway(RELOAD, true, false) {
		return errors.New(fmt.Sprintf("(IdxCreate %s) Failed to reload configuration", colName))
	}
	return nil
}

// Return list of all indexes
func (srv *Server) IdxAll(params []string) (jsonOrErr interface{}) {
	colName := params[1]
	if paths, exists := srv.ColIndexPathStr[colName]; exists {
		return paths
	} else {
		return errors.New(fmt.Sprintf("(IdxAll %s) Collection does not exist", colName))
	}
}

// Drop an index.
func (srv *Server) IdxDrop(params []string) (err interface{}) {
	colName := params[1]
	idxPath := params[2]
	// Verify that the collection exists
	if _, exists := srv.ColNumParts[colName]; !exists {
		return errors.New(fmt.Sprintf("(IdxDrop %s) Collection does not exist", colName))
	}
	// rm -rf index_directory
	if err = os.RemoveAll(path.Join(srv.DBDir, colName, HASHTABLE_DIRNAME_MAGIC+idxPath)); err != nil {
		return
	}
	// Reload my config
	if err = srv.Reload(nil); err != nil {
		return
	}
	// Inform other ranks to reload their config
	if !srv.BroadcastAway(RELOAD, true, false) {
		return errors.New(fmt.Sprintf("(IdxDrop %s) Failed to reload configuration", colName))
	}
	return nil
}

// Contact all ranks who own the collection to put the document on all indexes.
func (srv *Server) IndexDoc(colName string, docID uint64, doc interface{}) (err error) {
	numParts := uint64(srv.ColNumParts[colName])
	for i, indexPath := range srv.ColIndexPath[colName] {
		for _, toBeIndexed := range colpart.GetIn(doc, indexPath) {
			if toBeIndexed != nil {
				indexPathStr := srv.ColIndexPathStr[colName][i]
				// Figure out where to put it
				hashKey := colpart.StrHash(toBeIndexed)
				partNum := int(hashKey % numParts)
				if partNum == srv.Rank {
					// It belongs to my rank
					srv.Htables[colName][indexPathStr].Put(hashKey, docID)
				} else {
					// Go inter-rank: tell other rank to do the job
					if err = srv.InterRank[partNum].htPut(colName, indexPathStr, hashKey, docID); err != nil {
						return
					}
				}
			}
		}
	}
	return nil
}

// Contact all ranks who own the collection to remove the document from all indexes.
func (srv *Server) UnindexDoc(colName string, docID uint64, doc interface{}) (err error) {
	numParts := uint64(srv.ColNumParts[colName])
	for i, indexPath := range srv.ColIndexPath[colName] {
		for _, toBeIndexed := range colpart.GetIn(doc, indexPath) {
			if toBeIndexed != nil {
				indexPathStr := srv.ColIndexPathStr[colName][i]
				// Figure out where to put it
				hashKey := colpart.StrHash(toBeIndexed)
				partNum := int(hashKey % numParts)
				if partNum == srv.Rank {
					// It belongs to my rank
					srv.Htables[colName][indexPathStr].Remove(hashKey, docID)
				} else {
					// Go inter-rank: tell other rank to do the job
					if err = srv.InterRank[partNum].htPut(colName, indexPathStr, hashKey, docID); err != nil {
						return
					}
				}
			}
		}
	}
	return nil
}

// Insert a document and maintain hash index.
func (srv *Server) ColInsert(params []string) (uint64OrErr interface{}) {
	var err error
	colName := params[1]
	doc := params[2]
	// Validate parameters
	if _, exists := srv.ColNumParts[colName]; !exists {
		return errors.New(fmt.Sprintf("(ColInsert %s) Collection does not exist", colName))
	}
	var jsDoc map[string]interface{}
	if err = json.Unmarshal([]byte(doc), &jsDoc); err != nil || jsDoc == nil {
		return errors.New(fmt.Sprintf("(ColInsert %s) Client sent malformed JSON document", colName))
	}
	// Allocate an ID for the document
	docID := uid.NextUID()
	jsDoc[uid.PK_NAME] = strconv.FormatUint(docID, 10)
	// See where the document goes
	partNum := int(docID % uint64(srv.ColNumParts[colName]))
	if partNum == srv.Rank {
		// Oh I have it!
		if _, err = srv.ColParts[colName].Insert(jsDoc); err != nil {
			return err
		}
	} else {
		// Tell other rank to do it
		if _, err = srv.InterRank[partNum].docInsert(colName, jsDoc); err != nil {
			return err
		}
	}
	if err := srv.IndexDoc(colName, docID, jsDoc); err != nil {
		return err
	}
	return docID
}

// Get a document by its unique ID (Not physical ID).
func (srv *Server) ColGet(params []string) (jsonOrErr interface{}) {
	colName := params[1]
	docID := params[2]
	if _, exists := srv.ColNumParts[colName]; !exists {
		return errors.New(fmt.Sprintf("(ColGet %s) Collection does not exist", colName))
	}
	idInt, err := strconv.ParseUint(docID, 10, 64)
	if err != nil {
		return errors.New(fmt.Sprintf("(ColGet %s) Client sent malformed JSON document", colName))
	}
	var doc interface{}
	partNum := int(idInt % uint64(srv.ColNumParts[colName]))
	if partNum == srv.Rank {
		physID, err := srv.ColParts[colName].GetPhysicalID(idInt)
		if err != nil {
			return errors.New(fmt.Sprintf("Document %d does not exist in %s", idInt, colName))
		}
		if err = srv.ColParts[colName].Read(physID, &doc); err != nil {
			return err
		}
		return doc
	} else {
		doc, err := srv.InterRank[partNum].ColGet(colName, idInt)
		if err != nil {
			return err
		}
		return doc
	}
}

// Update a document in my rank, without maintaining index index.
func (srv *Server) ColUpdateNoIdx(params []string) (err interface{}) {
	colName := params[1]
	docID := params[2]
	doc := params[3]
	// Validate parameters
	if _, exists := srv.ColNumParts[colName]; !exists {
		return errors.New(fmt.Sprintf("(ColUpdateNoIdx %s) Collection does not exist", colName))
	}
	idInt, err := strconv.ParseUint(docID, 10, 64)
	if err != nil {
		return errors.New(fmt.Sprintf("(ColUpdateNoIdx %s) Client sent malformed ID: %s", colName, docID))
	}
	var newDoc map[string]interface{}
	if err = json.Unmarshal([]byte(doc), &newDoc); err != nil {
		return errors.New(fmt.Sprintf("(ColUpdateNoIdx %s) Client sent malformed JSON document", colName))
	}
	partNum := int(idInt % uint64(srv.ColNumParts[colName]))
	var originalDoc interface{}
	if partNum != srv.Rank {
		return errors.New(fmt.Sprintf("(ColUpdateNoIdx %s) My rank does not own the document", colName))
	}
	// Now my rank owns the document and go ahead to update the document
	// Make sure that client is not overwriting document ID
	newDoc[uid.PK_NAME] = docID
	// Read back the original document
	partition := srv.ColParts[colName]
	var originalPhysicalID uint64
	originalPhysicalID, err = srv.ColParts[colName].GetPhysicalID(idInt)
	if err == nil {
		partition.Read(originalPhysicalID, &originalDoc)
	} else {
		// The original document cannot be found - so we will insert the document instead of updating it
		tdlog.Printf("(ColUpdate %s) Cannot find the original document %d, will insert the updated document instead", idInt, colName)
	}
	// Overwrite the document
	if originalDoc == nil {
		// The original document cannot be found, so we do "repair" update
		if _, err = partition.Insert(newDoc); err != nil {
			return
		}
	} else {
		// Ordinary update
		if _, err = srv.ColParts[colName].Update(originalPhysicalID, newDoc); err != nil {
			return
		}
	}
	return
}

// Update a document and maintain hash index.
func (srv *Server) ColUpdate(params []string) (err interface{}) {
	colName := params[1]
	docID := params[2]
	doc := params[3]
	// Validate parameters
	if _, exists := srv.ColNumParts[colName]; !exists {
		return errors.New(fmt.Sprintf("(ColUpdate %s) Collection does not exist", colName))
	}
	idInt, err := strconv.ParseUint(docID, 10, 64)
	if err != nil {
		return errors.New(fmt.Sprintf("(ColUpdate %s) Client sent malformed ID: %s", colName, docID))
	}
	var newDoc map[string]interface{}
	if err = json.Unmarshal([]byte(doc), &newDoc); err != nil {
		return errors.New(fmt.Sprintf("(ColUpdate %s) Client sent malformed JSON document", colName))
	}
	partNum := int(idInt % uint64(srv.ColNumParts[colName]))
	var originalDoc interface{}
	if partNum == srv.Rank {
		// Now my rank owns the document and go ahead to update the document
		// Make sure that client is not overwriting document ID
		newDoc[uid.PK_NAME] = docID
		// Read back the original document
		partition := srv.ColParts[colName]
		var originalPhysicalID uint64
		originalPhysicalID, err = srv.ColParts[colName].GetPhysicalID(idInt)
		if err == nil {
			partition.Read(originalPhysicalID, &originalDoc)
		} else {
			// The original document cannot be found - so we will insert the document instead of updating it
			tdlog.Printf("(ColUpdate %s) Cannot find the original document %d, will insert the updated document instead", idInt, colName)
		}
		// Overwrite the document
		if originalDoc == nil {
			// The original document cannot be found, so we do "repair" update
			if _, err = partition.Insert(newDoc); err != nil {
				return
			}
		} else {
			// Ordinary update
			if _, err = srv.ColParts[colName].Update(originalPhysicalID, newDoc); err != nil {
				return
			}
		}
	} else {
		// If my rank does not own the document, coordinate this update with other ranks, and to prevent deadlock...
		// Contact other rank to get document content
		if originalDoc, err = srv.InterRank[partNum].ColGet(colName, idInt); err != nil {
			return
		}
		// Contact other rank to update document without maintaining index
		if err = srv.InterRank[partNum].colUpdateNoIdx(colName, idInt, newDoc); err != nil {
			return
		}
	}
	// No matter where the document is physically located at, my rank always coordinates index maintenance
	if err = srv.UnindexDoc(colName, idInt, originalDoc); err != nil {
		tdlog.Printf("ERROR Unindex doc %v %v %v", colName, idInt, originalDoc)
		return
	}
	return srv.IndexDoc(colName, idInt, newDoc)
}

// Delete a document by its unique ID (Not physical ID).
func (srv *Server) ColDeleteNoIdx(params []string) (err interface{}) {
	colName := params[1]
	docID := params[2]
	// Validate parameters
	if _, exists := srv.ColNumParts[colName]; !exists {
		return errors.New(fmt.Sprintf("(ColDeleteNoIdx %s) Collection does not exist", colName))
	}
	idInt, err := strconv.ParseUint(docID, 10, 64)
	if err != nil {
		return errors.New(fmt.Sprintf("(ColDeleteNoIdx %s) Client sent malformed ID: %s", colName, docID))
	}
	partNum := int(idInt % uint64(srv.ColNumParts[colName]))
	var originalDoc interface{}
	if partNum != srv.Rank {
		return errors.New(fmt.Sprintf("(ColDeleteNoIdx %s) My rank does not own the document", colName))
	}
	// Now my rank owns the document and go ahead to delete the document
	// Read back the original document
	partition := srv.ColParts[colName]
	var originalPhysicalID uint64
	originalPhysicalID, err = srv.ColParts[colName].GetPhysicalID(idInt)
	if err == nil {
		partition.Read(originalPhysicalID, &originalDoc)
	} else {
		// The original document cannot be found - so it has already been deleted
		return nil
	}
	// Delete the document
	srv.ColParts[colName].Delete(originalPhysicalID)
	return
}

// Delete a document by its unique ID (Not physical ID).
func (srv *Server) ColDelete(params []string) (err interface{}) {
	colName := params[1]
	docID := params[2]
	// Validate parameters
	if _, exists := srv.ColNumParts[colName]; !exists {
		return errors.New(fmt.Sprintf("(ColDelete %s) Collection does not exist", colName))
	}
	idInt, err := strconv.ParseUint(docID, 10, 64)
	if err != nil {
		return errors.New(fmt.Sprintf("(ColDelete %s) Client sent malformed ID: %s", colName, docID))
	}
	partNum := int(idInt % uint64(srv.ColNumParts[colName]))
	var originalDoc interface{}
	if partNum == srv.Rank {
		// Now my rank owns the document and go ahead to delete the document
		// Read back the original document
		partition := srv.ColParts[colName]
		var originalPhysicalID uint64
		originalPhysicalID, err = srv.ColParts[colName].GetPhysicalID(idInt)
		if err == nil {
			partition.Read(originalPhysicalID, &originalDoc)
		} else {
			// The original document cannot be found - so it has already been deleted
			return nil
		}
		// Delete the document
		srv.ColParts[colName].Delete(originalPhysicalID)
	} else {
		if err = srv.InterRank[partNum].colDeleteNoIdx(colName, idInt); err != nil {
			return
		}
	}
	// No matter where the document is physically located at, my rank always coordinates index maintenance
	return srv.UnindexDoc(colName, idInt, originalDoc)
}
