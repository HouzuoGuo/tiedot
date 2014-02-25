/* Server command implementations. */
package network

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/HouzuoGuo/tiedot/colpart"
	"github.com/HouzuoGuo/tiedot/dstruct"
	"github.com/HouzuoGuo/tiedot/tdlog"
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
func (server *Server) Reload() (err error) {
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
				tdlog.Panicf("Cannot figure out number of chunks for collection %s, manually repair it maybe? %v", server.DBDir, err)
			}
			server.ColNumParts[colName] = numchunks
			// Abort the program if total number of processes is not enough for a collection
			if server.TotalRank < numchunks {
				panic(fmt.Sprintf("Please start at least %d processes, because collection %s has %d partitions", numchunks, colName, numchunks))
			}
			// If my rank is within the numeric range of collection partitions, go ahead and open my part
			if server.Rank < numchunks {
				tdlog.Printf("My rank is %d and I am going to open my partition in %s", server.Rank, f.Name())
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
							tdlog.Printf("My rank is %d and I am going to open my partition in hashtable %s", server.Rank, info.Name())
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
func (srv *Server) FlushAll(_ interface{}) (err interface{}) {
	for _, part := range srv.ColParts {
		if err = part.Flush(); err != nil {
			return
		}
	}
	for _, htMap := range srv.Htables {
		for _, ht := range htMap {
			if err = ht.File.Flush(); err != nil {
				return
			}
		}
	}
	return nil
}

// Create a collection.
func (srv *Server) ColCreate(input interface{}) (err interface{}) {
	params := input.([]string)
	colName := params[1]
	numParts := params[2]
	if err = srv.FlushAll(nil); err != nil {
		return
	}
	// Check input parameters
	if strings.Contains(colName, " ") {
		return errors.New(fmt.Sprintf("Sorry! Collection name may not contain space (You requested %s)", colName))
	}
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
	// Reload config
	return srv.Reload()
}

// Return all collection names in JSON.
func (src *Server) ColAll(_ interface{}) (strOrErr interface{}) {
	names := make([]string, len(src.ColNumParts))
	i := 0
	for name, _ := range src.ColNumParts {
		names[i] = name
		i++
	}
	if namesJSON, err := json.Marshal(names); err != nil {
		return err
	} else {
		return string(namesJSON)
	}
}

// Ping, Ping1, PingJS are for testing purpose, they do not manipulate any data.
func (src *Server) Ping(_ interface{}) (strNoError interface{}) {
	return ACK
}
func (src *Server) Ping1(_ interface{}) (uint64NoError interface{}) {
	return uint64(1)
}
func (src *Server) PingJS(_ interface{}) (jsonNoError interface{}) {
	return []string{ACK, ACK}
}
func (src *Server) PingErr(_ interface{}) (mustBErr interface{}) {
	return errors.New("this is an error")
}
