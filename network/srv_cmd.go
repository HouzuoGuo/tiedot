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
			// Abort the program if total number of processes is not enough for a collection
			if server.TotalRank < numchunks {
				panic(fmt.Sprintf("Please start at least %d processes, because collection %s has %d partitions", numchunks, colName, numchunks))
			}
			// If my rank is within the numeric range of collection partitions, go ahead and open my part
			if server.Rank < numchunks {
				tdlog.Printf("Rank %d: I am going to open my partition in %s", server.Rank, f.Name())
				// Open data partition
				colDir := path.Join(server.DBDir, colName)
				part, err := colpart.OpenPart(path.Join(colDir, CHUNK_DIRNAME_MAGIC+strconv.Itoa(server.Rank)))
				if err != nil {
					return err
				}
				// Put the partition into server structure
				server.ColParts[colName] = part
				server.Htables[colName] = make(map[string]*dstruct.HashTable)
				// Look for indexes in the collection
				walker := func(_ string, info os.FileInfo, err2 error) error {
					if err2 != nil {
						tdlog.Error(err)
						return nil
					}
					if info.IsDir() {
						switch {
						case strings.HasPrefix(info.Name(), HASHTABLE_DIRNAME_MAGIC):
							tdlog.Printf("Rank %d: I am going to open my partition in hashtable %s", server.Rank, info.Name())
							// Figure out indexed path - including the partition number
							indexPathStr := info.Name()[len(HASHTABLE_DIRNAME_MAGIC):]
							indexPath := strings.Split(indexPathStr, INDEX_PATH_SEP)
							// Open a hash table index and put it into collection structure
							ht, err := dstruct.OpenHash(path.Join(colDir, info.Name(), strconv.Itoa(server.Rank)), indexPath)
							if err != nil {
								return err
							}
							server.Htables[colName][indexPathStr] = ht
						}
					}
					return nil
				}
				err = filepath.Walk(colDir, walker)
			}
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
func (srv *Server) DocInsert(params []string) (err interface{}) {
	colName := params[1]
	jsonDoc := params[2]
	// Check input collection name and JSON document string
	if col, exists := srv.ColParts[colName]; !exists {
		return errors.New(fmt.Sprintf("Sorry! Collection '%s' may exist, but my rank does not own a partition of it. Double check the name and try a lower rank.", colName))
	} else {
		var doc map[string]interface{}
		if err = json.Unmarshal([]byte(jsonDoc), &doc); err != nil {
			return errors.New(fmt.Sprintf("DocInsert input JSON is not well formed: '%s'", jsonDoc))
		}
		// Insert the document into my partition
		if _, err = col.Insert(doc); err != nil {
			return errors.New(fmt.Sprintf("Cannot insert document into %s, error: %v", colName, err))
		}
	}
	return nil
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
		if physID, err := col.GetPhysicalID(idInt); err != nil {
			return err
		} else {
			if jsonStr, err := col.ReadStr(physID); err != nil {
				return err
			} else {
				return jsonStr
			}
		}
	}
}

// Update a document in my partition.
func (srv *Server) DocUpdate(params []string) (err interface{}) {
	colName := params[1]
	id := params[2]
	jsonDoc := params[3]
	// Check input collection name, new document JSON, and UID
	idInt, err := strconv.ParseUint(id, 10, 64)
	if err != nil {
		return errors.New(fmt.Sprintf("%s is not a valid document ID", id))
	}
	if col, exists := srv.ColParts[colName]; !exists {
		return errors.New(fmt.Sprintf("Sorry! Collection '%s' may exist, but my rank does not own a partition of it. Double check the name and try a lower rank.", colName))
	} else {
		var doc map[string]interface{}
		if err = json.Unmarshal([]byte(jsonDoc), &doc); err != nil {
			return errors.New(fmt.Sprintf("DocUpdate input JSON is not well formed: '%s'", jsonDoc))
		}
		doc[uid.PK_NAME] = id // client is not supposed to change UID, just to make sure
		var physID uint64
		if physID, err = col.GetPhysicalID(idInt); err != nil {
			return err
		} else {
			if _, err = col.Update(physID, doc); err != nil {
				return
			}
		}
	}
	return nil
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
		if physID, err := col.GetPhysicalID(idInt); err != nil {
			return err
		} else {
			col.Delete(physID)
		}
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
func (srv *Server) HTGet(params []string) (strOrRrr interface{}) {
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
			if keyInt, strOrRrr = strconv.ParseUint(key, 10, 64); strOrRrr != nil {
				return
			}
			if limitInt, strOrRrr = strconv.ParseUint(limit, 10, 64); strOrRrr != nil {
				return
			}
			// Assemble response into "key1 key2 key3 val1 val2 val3 ..."
			keys, vals := ht.Get(keyInt, limitInt)
			resp := make([]string, len(keys)*2)
			for i, key := range keys {
				resp[i] = strconv.FormatUint(key, 10)
			}
			for i, val := range vals {
				resp[i+len(keys)] = strconv.FormatUint(val, 10)
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
