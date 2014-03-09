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
		return errors.New(fmt.Sprintf("Sorry! There can be at most %d partitions because there are only %d processes running", srv.TotalRank, srv.TotalRank))
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
		return errors.New("Failed to reload configuration, check server logs for clue please")
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
		return errors.New(fmt.Sprintf("Sorry! New name may not be the same as old name (trying to rename %s)", oldName))
	}
	if _, alreadyExists := srv.ColNumParts[newName]; alreadyExists {
		return errors.New(fmt.Sprintf("Sorry! New name is already used (trying to rename %s)", oldName))
	}
	if _, exists := srv.ColNumParts[oldName]; !exists {
		return errors.New(fmt.Sprintf("Sorry! Old name does not exist (trying to rename %s)", oldName))
	}
	// Rename collection directory
	if err = os.Rename(path.Join(srv.DBDir, oldName), path.Join(srv.DBDir, newName)); err != nil {
		return
	}
	// Reload myself and inform other ranks to reload their config
	srv.Reload(nil)
	if !srv.BroadcastAway(RELOAD, true, false) {
		return errors.New("Failed to reload configuration, check server logs for clue please")
	}
	return nil
}

// Drop a collection.
func (srv *Server) ColDrop(params []string) (err interface{}) {
	colName := params[1]
	// Check input name
	if _, exists := srv.ColNumParts[colName]; !exists {
		return errors.New(fmt.Sprintf("Sorry! Collection name does not exist (trying to drop %s)", colName))
	}
	// Remove the collection from file system
	if err = os.RemoveAll(path.Join(srv.DBDir, colName)); err != nil {
		return
	}
	// Reload myself and inform other ranks to reload their config
	srv.Reload(nil)
	if !srv.BroadcastAway(RELOAD, true, false) {
		return errors.New("Failed to reload configuration, check server logs for clue please")
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
		return errors.New(fmt.Sprintf("Sorry! Collection '%s' may exist, but my rank does not own a partition of it. Double check the name and try a lower rank.", colName))
	} else {
		var doc map[string]interface{}
		if strOrErr = json.Unmarshal([]byte(jsonDoc), &doc); strOrErr != nil {
			return errors.New(fmt.Sprintf("DocInsert input JSON is not well formed: '%s'", jsonDoc))
		}
		// Insert the document into my partition
		var newDocID uint64
		if newDocID, strOrErr = col.Insert(doc); strOrErr != nil {
			return errors.New(fmt.Sprintf("Cannot insert document into %s, error: %v", colName, strOrErr))
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
		return errors.New(fmt.Sprintf("%s is not a valid document ID", id))
	}
	if col, exists := srv.ColParts[colName]; !exists {
		return errors.New(fmt.Sprintf("Sorry! Collection '%s' may exist, but my rank does not own a partition of it. Double check the name and try a lower rank.", colName))
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
		return errors.New(fmt.Sprintf("%s is not a valid document ID", id))
	}
	if col, exists := srv.ColParts[colName]; !exists {
		return errors.New(fmt.Sprintf("Sorry! Collection '%s' may exist, but my rank does not own a partition of it. Double check the name and try a lower rank.", colName))
	} else {
		var doc map[string]interface{}
		if strOrErr = json.Unmarshal([]byte(jsonDoc), &doc); strOrErr != nil {
			return errors.New(fmt.Sprintf("DocUpdate input JSON is not well formed: '%s'", jsonDoc))
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
		return errors.New(fmt.Sprintf("%s is not a valid document ID", id))
	}
	if col, exists := srv.ColParts[colName]; !exists {
		return errors.New(fmt.Sprintf("Sorry! Collection '%s' may exist, but my rank does not own a partition of it. Double check the name and try a lower rank.", colName))
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
		return errors.New(fmt.Sprintf("Sorry! Collection '%s' may exist, but my rank does not own a partition of it. Double check the name and try a lower rank.", colName))
	} else {
		if ht, exists := col[htName]; !exists {
			return errors.New(fmt.Sprintf("Hash table %s does not exist in %s", htName, colName))
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
		return errors.New(fmt.Sprintf("Sorry! Collection '%s' may exist, but my rank does not own a partition of it. Double check the name and try a lower rank.", colName))
	} else {
		if ht, exists := col[htName]; !exists {
			return errors.New(fmt.Sprintf("Hash table %s does not exist in %s", htName, colName))
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
		return errors.New(fmt.Sprintf("Sorry! Collection '%s' may exist, but my rank does not own a partition of it. Double check the name and try a lower rank.", colName))
	} else {
		if ht, exists := col[htName]; !exists {
			return errors.New(fmt.Sprintf("Hash table %s does not exist in %s", htName, colName))
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
		return errors.New(fmt.Sprintf("Collection %s does not exist", colName))
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
		return errors.New("Failed to reload configuration, check server logs for clue please")
	}
	return nil
}

// Return list of all indexes
func (srv *Server) IdxAll(params []string) (jsonOrErr interface{}) {
	colName := params[1]
	if paths, exists := srv.ColIndexPathStr[colName]; exists {
		return paths
	} else {
		return errors.New(fmt.Sprintf("Collection %s does not exist", colName))
	}
}

// Drop an index.
func (srv *Server) IdxDrop(params []string) (err interface{}) {
	colName := params[1]
	idxPath := params[2]
	// Verify that the collection exists
	if _, exists := srv.ColNumParts[colName]; !exists {
		return errors.New(fmt.Sprintf("Collection %s does not exist", colName))
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
		return errors.New("Failed to reload configuration, check server logs for clue please")
	}
	return nil
}

// Contact all ranks who own the collection to put the document on all indexes.
func (srv *Server) IndexDoc(colName string, docID uint64, doc interface{}) (err error) {
	for i, indexPath := range srv.ColIndexPath[colName] {
		for _, toBeIndexed := range colpart.GetIn(doc, indexPath) {
			if toBeIndexed != nil {
				indexPathStr := srv.ColIndexPathStr[colName][i]
				// Figure out where to put it
				hashKey := colpart.StrHash(toBeIndexed)
				partNum := int(hashKey % uint64(srv.TotalRank))
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
	// Similar to IndexDoc
	for i, indexPath := range srv.ColIndexPath[colName] {
		for _, toBeIndexed := range colpart.GetIn(doc, indexPath) {
			if toBeIndexed != nil {
				indexPathStr := srv.ColIndexPathStr[colName][i]
				hashKey := colpart.StrHash(toBeIndexed)
				partNum := int(hashKey % uint64(srv.TotalRank))
				if partNum == srv.Rank {
					srv.Htables[colName][indexPathStr].Remove(hashKey, docID)
				} else {
					if err = srv.InterRank[partNum].htDelete(colName, indexPathStr, hashKey, docID); err != nil {
						return
					}
				}
			}
		}
	}
	return nil
}

// Insert a document and maintain hash index.
func (srv *Server) ColInsert(params []string) (strOrErr interface{}) {
	colName := params[1]
	doc := params[2]
	// Validate parameters
	if _, exists := srv.ColNumParts[colName]; !exists {
		return errors.New(fmt.Sprintf("Collection %s does not exist", colName))
	}
	var jsDoc map[string]interface{}
	if strOrErr = json.Unmarshal([]byte(doc), &jsDoc); strOrErr != nil {
		return errors.New(fmt.Sprintf("Client sent malformed JSON document"))
	}
	// Allocate an ID for the document
	docID := uid.NextUID()
	// See where the document goes
	partNum := int(docID % uint64(srv.TotalRank))
	var physicalID uint64
	if partNum == srv.Rank {
		// Oh I have it!
		if physicalID, strOrErr = srv.ColParts[colName].Insert(jsDoc); strOrErr != nil {
			return
		}
	} else {
		// Tell other rank to do it
		if physicalID, strOrErr = srv.InterRank[partNum].docInsert(colName, jsDoc); strOrErr != nil {
			return
		}
	}
	return srv.IndexDoc(colName, physicalID, jsDoc)
}

// Get a document by its unique ID (Not physical ID).
func (srv *Server) ColGet(params []string) (jsonOrErr interface{}) {
	colName := params[1]
	docID := params[2]
	if _, exists := srv.ColNumParts[colName]; !exists {
		return errors.New(fmt.Sprintf("Collection %s does not exist", colName))
	}
	idInt, err := strconv.ParseUint(docID, 10, 64)
	if err != nil {
		return errors.New(fmt.Sprintf("Client sent malformed document ID"))
	}
	var doc interface{}
	partNum := int(idInt % uint64(srv.TotalRank))
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

// Update a document and maintain hash index.
func (srv *Server) ColUpdate(params []string) (err interface{}) {
	colName := params[1]
	docID := params[2]
	doc := params[3]
	// Validate parameters
	if _, exists := srv.ColNumParts[colName]; !exists {
		return errors.New(fmt.Sprintf("Collection %s does not exist", colName))
	}
	idInt, err := strconv.ParseUint(docID, 10, 64)
	if err != nil {
		return errors.New(fmt.Sprintf("Client sent malformed document ID"))
	}
	var newDoc map[string]interface{}
	if err = json.Unmarshal([]byte(doc), &newDoc); err != nil {
		return errors.New(fmt.Sprintf("Client sent malformed JSON document"))
	}
	// Update is a three-step process...
	// first, read back the original document
	var originalDoc interface{}
	partNum := int(idInt % uint64(srv.TotalRank))
	if partNum == srv.Rank {
		physID, err := srv.ColParts[colName].GetPhysicalID(idInt)
		if err == nil {
			srv.ColParts[colName].Read(physID, &originalDoc)
		}
	} else {
		originalDoc, _ = srv.InterRank[partNum].ColGet(colName, idInt)
	}
	// second, overwrite the document
	var newPhysicalID uint64
	if partNum == srv.Rank {
		// I have it..
		if newPhysicalID, err = srv.ColParts[colName].Update(idInt, newDoc); err != nil {
			return
		}
	} else {
		if newPhysicalID, err = srv.InterRank[partNum].docUpdate(colName, idInt, newDoc); err != nil {
			return
		}
	}
	// third, update indexes
	if originalDoc == nil {
		tdlog.Printf("Trying to update document %d in %s: will not attempt to unindex the old document, as it cannot be read back", idInt, colName)
	} else if err = srv.UnindexDoc(colName, idInt, originalDoc); err != nil {
		return
	}
	return srv.IndexDoc(colName, newPhysicalID, newDoc)
}

// Delete a document by its unique ID (Not physical ID).
func (srv *Server) ColDelete(params []string) (err interface{}) {
	colName := params[1]
	docID := params[2]
	// Validate parameters
	if _, exists := srv.ColNumParts[colName]; !exists {
		return errors.New(fmt.Sprintf("Collection %s does not exist", colName))
	}
	idInt, err := strconv.ParseUint(docID, 10, 64)
	if err != nil {
		return errors.New(fmt.Sprintf("Client sent malformed document ID"))
	}
	// Update is a three-step process...
	// first, read back the original document
	var originalDoc interface{}
	partNum := int(idInt % uint64(srv.TotalRank))
	if partNum == srv.Rank {
		physID, err := srv.ColParts[colName].GetPhysicalID(idInt)
		if err == nil {
			srv.ColParts[colName].Read(physID, &originalDoc)
		}
	} else {
		originalDoc, _ = srv.InterRank[partNum].ColGet(colName, idInt)
	}
	// second, delete the document
	if partNum == srv.Rank {
		// I have it..
		srv.ColParts[colName].Delete(idInt)
	} else {
		if err = srv.InterRank[partNum].docDelete(colName, idInt); err != nil {
			return
		}
	}
	// third, update index
	return srv.UnindexDoc(colName, idInt, originalDoc)
}
