// Manage files and directories of a sharded database.
package data

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
)

const (
	// The string identifier of DB directory structure
	CURRENT_VERSION = "201501"

	VERSION_FILE = "version"
	NSHARDS_FILE = "nshards"

	COLLECTION_DIR       = "collections"
	COLLECTION_INDEX_DIR = "indexes"
	INDEX_PATH_SEP       = "!"

	COLLECTION_DOC_DATA_FILE  = "documents"
	COLLECTION_ID_LOOKUP_FILE = "document-ids"
)

// Join index path segments together to form an index file name.
func JoinIndexPath(idxPath []string) string {
	return strings.Join(idxPath, INDEX_PATH_SEP)
}

// Split an index file name into path segments.
func SplitIndexPath(jointIdxPath string) []string {
	return strings.Split(jointIdxPath, INDEX_PATH_SEP)
}

// The directory structure of sharded database (all shards).
type DBDirStruct struct {
	NShards     int
	Version     string
	DBDir       string
	Collections []string
	Indexes     map[string][]string
}

// Return all collection names, sorted alphabetically.
func (dbfs *DBDirStruct) GetCollectionNamesSorted() (ret []string) {
	ret = make([]string, len(dbfs.Collections))
	copy(ret, dbfs.Collections)
	sort.Strings(ret)
	return
}

// Return all indexes (paths are joint), sorted alphabetically.
func (dbfs *DBDirStruct) GetIndexesSorted(colName string) (ret []string) {
	colIndexes := dbfs.Indexes[colName]
	if colIndexes == nil {
		return
	}
	ret = make([]string, len(colIndexes))
	copy(ret, colIndexes)
	sort.Strings(ret)
	return
}

// Return path to document data and ID lookup files of the specific shard and collection.
func (dbfs *DBDirStruct) GetCollectionDataFilePaths(colName string, shard int) (dataFile, idLookupFile string) {
	colDir := path.Join(dbfs.DBDir, strconv.Itoa(shard), COLLECTION_DIR, colName)
	return path.Join(colDir, COLLECTION_DOC_DATA_FILE), path.Join(colDir, COLLECTION_ID_LOOKUP_FILE)
}

// Return path to hash table index file of the specific shard collection, and index name.
func (dbfs *DBDirStruct) GetIndexFilePath(colName string, idxPath []string, shard int) string {
	return path.Join(dbfs.DBDir, strconv.Itoa(shard), COLLECTION_DIR, colName, JoinIndexPath(idxPath))
}

// Create directories and empty data files for a new collection.
func (dbfs *DBDirStruct) CreateCollection(colName string) error {
	for _, existingCol := range dbfs.Collections {
		if colName == existingCol {
			return fmt.Errorf("Collection %s already exists", colName)
		}
	}
	for i := 0; i < dbfs.NShards; i++ {
		colDir := path.Join(dbfs.DBDir, strconv.Itoa(i), COLLECTION_DIR, colName)
		if err := os.MkdirAll(colDir, 0700); err != nil {
			return err
		} else if err := os.MkdirAll(path.Join(colDir, COLLECTION_INDEX_DIR), 0700); err != nil {
			return err
		} else if err := ioutil.WriteFile(path.Join(colDir, COLLECTION_DOC_DATA_FILE), []byte{}, 0600); err != nil {
			return err
		} else if err := ioutil.WriteFile(path.Join(colDir, COLLECTION_ID_LOOKUP_FILE), []byte{}, 0600); err != nil {
			return err
		}
	}
	dbfs.Collections = append(dbfs.Collections, colName)
	dbfs.Indexes[colName] = make([]string, 0, 0)
	return nil
}

// Rename a collection.
func (dbfs *DBDirStruct) RenameCollection(oldName, newName string) error {
	foundAt := -1
	for i, existingCol := range dbfs.Collections {
		if existingCol == newName {
			return fmt.Errorf("Collection name %s is already used", newName)
		} else if existingCol == oldName {
			foundAt = i
		}
	}
	if foundAt == -1 {
		return fmt.Errorf("Collection %s does not exist", oldName)
	}
	for i := 0; i < dbfs.NShards; i++ {
		colOldDir := path.Join(dbfs.DBDir, strconv.Itoa(i), COLLECTION_DIR, oldName)
		colNewDir := path.Join(dbfs.DBDir, strconv.Itoa(i), COLLECTION_DIR, newName)
		if err := os.Rename(colOldDir, colNewDir); err != nil {
			return err
		}
	}
	dbfs.Collections[foundAt] = newName
	dbfs.Indexes[newName] = dbfs.Indexes[oldName]
	delete(dbfs.Indexes, oldName)
	return nil
}

// Remove directories and data files of a collection.
func (dbfs *DBDirStruct) DropCollection(colName string) error {
	foundAt := -1
	for i, existingCol := range dbfs.Collections {
		if colName == existingCol {
			foundAt = i
			break
		}
	}
	if foundAt == -1 {
		return fmt.Errorf("Collection %s does not exist", colName)
	}
	for i := 0; i < dbfs.NShards; i++ {
		if err := os.RemoveAll(path.Join(dbfs.DBDir, strconv.Itoa(i), COLLECTION_DIR, colName)); err != nil {
			return err
		}
	}
	dbfs.Collections = append(dbfs.Collections[:foundAt], dbfs.Collections[foundAt+1:]...)
	delete(dbfs.Indexes, colName)
	return nil
}

// Create an empty file for a new index.
func (dbfs *DBDirStruct) CreateIndex(colName, jointIdxPath string) error {
	if colIndexes, colExists := dbfs.Indexes[colName]; !colExists {
		return fmt.Errorf("Collection %s does not exist", colName)
	} else {
		for _, existingIndex := range colIndexes {
			if jointIdxPath == existingIndex {
				return fmt.Errorf("Index %s already exists in %s", jointIdxPath, colName)
			}
		}
	}
	for i := 0; i < dbfs.NShards; i++ {
		colDir := path.Join(dbfs.DBDir, strconv.Itoa(i), COLLECTION_DIR, colName)
		if err := ioutil.WriteFile(path.Join(colDir, COLLECTION_INDEX_DIR, jointIdxPath), []byte{}, 0600); err != nil {
			return err
		}
	}
	dbfs.Indexes[colName] = append(dbfs.Indexes[colName], jointIdxPath)
	return nil
}

// Remove an index file.
func (dbfs *DBDirStruct) DropIndex(colName, jointIdxPath string) error {
	idxFoundAt := -1
	if colIndexes, colExists := dbfs.Indexes[colName]; !colExists {
		return fmt.Errorf("Collection %s does not exist", colName)
	} else {
		for i, existingIndex := range colIndexes {
			if jointIdxPath == existingIndex {
				idxFoundAt = i
				break
			}
		}
	}
	if idxFoundAt == -1 {
		return fmt.Errorf("Index %s does not exist in %s", jointIdxPath, colName)
	}
	for i := 0; i < dbfs.NShards; i++ {
		idxFile := path.Join(dbfs.DBDir, strconv.Itoa(i), COLLECTION_DIR, colName, COLLECTION_INDEX_DIR, jointIdxPath)
		if err := os.Remove(idxFile); err != nil {
			return err
		}
	}
	dbfs.Indexes[colName] = append(dbfs.Indexes[colName][:idxFoundAt], dbfs.Indexes[colName][idxFoundAt+1:]...)
	return nil
}

// Identify whether a directory hosts a database, and whether the DB version is current/latest.
func DBIdentify(dir string) (dirExists, latestVersion bool, err error) {
	if _, err = os.Stat(dir); os.IsNotExist(err) {
		return false, false, nil
	} else if err != nil {
		return false, false, err
	}
	verInfo, err := ioutil.ReadFile(path.Join(dir, VERSION_FILE))
	if err != nil {
		return true, false, nil
	}
	return true, strings.TrimSpace(string(verInfo)) == CURRENT_VERSION, nil
}

// Create a database directory structure.
func DBNewDir(dir string, nShards int) error {
	dirExists, latestVersion, err := DBIdentify(dir)
	if dirExists && !latestVersion || err != nil {
		return fmt.Errorf("The directory already hosts an older DB version, or file operation failed.")
	}
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	for i := 0; i < nShards; i++ {
		if err := os.MkdirAll(path.Join(dir, strconv.Itoa(i)), 0700); err != nil {
			return err
		} else if err := os.MkdirAll(path.Join(dir, strconv.Itoa(i), COLLECTION_DIR), 0700); err != nil {
			return err
		}
	}
	if err := ioutil.WriteFile(path.Join(dir, VERSION_FILE), []byte(CURRENT_VERSION), 0600); err != nil {
		return err
	} else if err := ioutil.WriteFile(path.Join(dir, NSHARDS_FILE), []byte(strconv.Itoa(nShards)), 0600); err != nil {
		return err
	}
	return nil
}

// Read the content of sharded database directory and return the database structure info.
func DBReadDir(dir string) (dbfs *DBDirStruct, err error) {
	dbfs = &DBDirStruct{DBDir: dir}
	verBytes, err := ioutil.ReadFile(path.Join(dir, VERSION_FILE))
	if err != nil {
		return
	}
	dbfs.Version = strings.TrimSpace(string(verBytes))
	if dbfs.Version != CURRENT_VERSION {
		return nil, fmt.Errorf("The DB file version is %s in %s, which is incompatible with current version %s.", dbfs.Version, dir, CURRENT_VERSION)
	}
	nshardsBytes, err := ioutil.ReadFile(path.Join(dir, NSHARDS_FILE))
	if err != nil {
		return
	}
	dbfs.NShards, err = strconv.Atoi(strings.TrimSpace(string(nshardsBytes)))
	if err != nil {
		return
	}
	// Open collections directory
	colDirContent, err := ioutil.ReadDir(path.Join(dir, "0", COLLECTION_DIR))
	if err != nil {
		return
	}
	colNames := make([]string, 0, 0)
	indexes := make(map[string][]string)
	for _, colDirName := range colDirContent {
		colName := colDirName.Name()
		indexes[colName] = make([]string, 0, 0)
		colNames = append(colNames, colName)
		// Open "indexes" directory inside the collection directory
		indexDirContent, err := ioutil.ReadDir(path.Join(dir, "0", COLLECTION_DIR, colName, COLLECTION_INDEX_DIR))
		if err != nil {
			return nil, err
		}
		for _, indexFileName := range indexDirContent {
			indexes[colName] = append(indexes[colName], indexFileName.Name())
		}
	}
	dbfs.Collections = colNames
	dbfs.Indexes = indexes
	return
}
