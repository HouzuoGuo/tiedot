/* Single-shard - document management and index maintenance features */
/* Single shard - collection and DB storage management features. */
/* Single shard - collection schema and index management features. */
/* Features supporting multi-shard environment in addition to the ordinary single-shard implementation. */
package db

import (
	"fmt"
	"github.com/HouzuoGuo/tiedot/data"
	"github.com/HouzuoGuo/tiedot/tdlog"
	"io"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
)

// Tie schema and database files together to provide collection/index manipulation features.
type DB struct {
	// Root path of database directory
	dir    string
	schema *Schema
	dbfs   *data.DBDirStruct

	colByID   map[int32]*data.Partition
	indexByID map[int32]*data.HashTable

	// Currently held document locks
	lockedDocs map[uint64]struct{}
}

// Open database, load schema and data structures.
func OpenDB(dir string) (*DB, error) {
	db := &DB{
		dir:        dir,
		schema:     new(Schema),
		colByID:    make(map[int32]*data.Partition),
		indexByID:  make(map[int32]*data.HashTable),
		lockedDocs: make(map[uint64]struct{})}
	return db, db.Reload()
}

// Load database schema and data structures
func (db *DB) Reload() (err error) {
	if db.dbfs, err = data.DBReadDir(dir); err != nil {
		return
	}
	// else if err = db.schema.ReloadAndSetRev()

	// Look for collection directories and open the collections
	db.cols = make(map[string]*Col)
	dirContent, err := ioutil.ReadDir(db.path)
	if err != nil {
		return err
	}
	for _, maybeColDir := range dirContent {
		if !maybeColDir.IsDir() {
			continue
		}
		if db.cols[maybeColDir.Name()], err = OpenCol(db, maybeColDir.Name()); err != nil {
			return err
		}
	}
	return err
}

// Create a new collection.
func (db *DB) Create(name string) error {
	if _, exists := db.cols[name]; exists {
		return fmt.Errorf("Collection %s already exists", name)
	} else if err := os.MkdirAll(path.Join(db.path, name), 0700); err != nil {
		return err
	} else if db.cols[name], err = OpenCol(db, name); err != nil {
		return err
	}
	return nil
}

// Return all collection names.
func (db *DB) AllCols() (ret []string) {
	ret = make([]string, 0, len(db.cols))
	for name, _ := range db.cols {
		ret = append(ret, name)
	}
	sort.Strings(ret)
	return
}

// Return a reference to collection structure, which can be used to manage documents and collection schema.
func (db *DB) Use(name string) *Col {
	if col, exists := db.cols[name]; exists {
		return col
	}
	return nil
}

// Rename a collection.
func (db *DB) Rename(oldName, newName string) error {
	if _, exists := db.cols[oldName]; !exists {
		return fmt.Errorf("Collection %s does not exist", oldName)
	} else if _, exists := db.cols[newName]; exists {
		return fmt.Errorf("Collection %s already exists", newName)
	} else if newName == oldName {
		return fmt.Errorf("Old and new names are the same")
	} else if err := db.cols[oldName].close(); err != nil {
		return err
	} else if err := os.Rename(path.Join(db.path, oldName), path.Join(db.path, newName)); err != nil {
		return err
	} else if db.cols[newName], err = OpenCol(db, newName); err != nil {
		return err
	}
	delete(db.cols, oldName)
	return nil
}

// Truncate a collection - delete all documents.
func (db *DB) Truncate(name string) error {
	if _, exists := db.cols[name]; !exists {
		return fmt.Errorf("Collection %s does not exist", name)
	}
	col := db.cols[name]
	if err := col.part.Clear(); err != nil {
		return err
	}
	for _, ht := range col.hts {
		if err := ht.Clear(); err != nil {
			return err
		}
	}
	return nil
}

// Drop a collection and lose all of its documents and indexes.
func (db *DB) Drop(name string) error {
	if _, exists := db.cols[name]; !exists {
		return fmt.Errorf("Collection %s does not exist", name)
	} else if err := db.cols[name].close(); err != nil {
		return err
	} else if err := os.RemoveAll(path.Join(db.path, name)); err != nil {
		return err
	}
	delete(db.cols, name)
	return nil
}

// Close all database files. This DB reference cannot be used any longer.
func (db *DB) Close() error {
	errs := make([]error, 0, 0)
	for _, col := range db.cols {
		if err := col.close(); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) == 0 {
		return nil
	}
	return fmt.Errorf("%v", errs)
}

// Copy this database into destination directory (for backup).
func (db *DB) Dump(dest string) error {
	cpFun := func(currPath string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			relPath, err := filepath.Rel(db.path, currPath)
			if err != nil {
				return err
			}
			destDir := path.Join(dest, relPath)
			if err := os.MkdirAll(destDir, 0700); err != nil {
				return err
			}
			tdlog.Noticef("Dump: created directory %s", destDir)
		} else {
			src, err := os.Open(currPath)
			if err != nil {
				return err
			}
			relPath, err := filepath.Rel(db.path, currPath)
			if err != nil {
				return err
			}
			destPath := path.Join(dest, relPath)
			if _, fileExists := os.Open(destPath); fileExists == nil {
				return fmt.Errorf("Destination file %s already exists", destPath)
			}
			destFile, err := os.Create(destPath)
			if err != nil {
				return err
			}
			written, err := io.Copy(destFile, src)
			if err != nil {
				return err
			}
			tdlog.Noticef("Dump: copied file %s, size is %d", destPath, written)
		}
		return nil
	}
	return filepath.Walk(db.path, cpFun)
}

// Open a collection and load schema information.
func OpenCol(db *DB, name string) (*Col, error) {
	col := &Col{db: db, name: name, locked: make(map[uint64]struct{})}
	if err := os.MkdirAll(path.Join(col.db.path, col.name), 0700); err != nil {
		return nil, err
	}
	return col, col.load()
}

// Load collection schema (index schema).
func (col *Col) load() error {
	col.hts = make(map[string]*data.HashTable)
	col.indexPaths = make(map[string][]string)
	// Open document collection
	var err error
	if col.part, err = data.OpenPartition(
		path.Join(col.db.path, col.name, DOC_DATA_FILE),
		path.Join(col.db.path, col.name, DOC_LOOKUP_FILE)); err != nil {
		return err
	}
	// Search for index files
	colDirContent, err := ioutil.ReadDir(path.Join(col.db.path, col.name))
	if err != nil {
		return err
	}
	for _, htFile := range colDirContent {
		if htFile.IsDir() || htFile.Name() == DOC_DATA_FILE || htFile.Name() == DOC_LOOKUP_FILE {
			continue
		}
		// Open index file
		idxName := htFile.Name()
		idxPath := strings.Split(idxName, INDEX_PATH_SEP)
		col.indexPaths[idxName] = idxPath
		if col.hts[idxName], err = data.OpenHashTable(
			path.Join(col.db.path, col.name, idxName)); err != nil {
			return err
		}
	}
	return nil
}

// Close all collection files. Stop using the collection after the call.
func (col *Col) close() error {
	errs := make([]error, 0, 0)
	if err := col.part.Close(); err != nil {
		errs = append(errs, err)
	}
	for _, ht := range col.hts {
		if err := ht.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) == 0 {
		return nil
	}
	return fmt.Errorf("%v", errs)
}

// Partition documents into roughly equally sized portions, and run the function on each document in the portion.
func (col *Col) ForEachDocInPage(page, total uint64, fun func(id uint64, doc []byte) bool) {
	if !col.part.ForEachDoc(page, total, fun) {
		return
	}
}

// Return all indexed paths.
func (col *Col) AllIndexes() (ret [][]string) {
	ret = make([][]string, 0, len(col.indexPaths))
	for _, path := range col.indexPaths {
		pathCopy := make([]string, len(path))
		for i, p := range path {
			pathCopy[i] = p
		}
		ret = append(ret, pathCopy)
	}
	return ret
}

// Return all indexed paths. Index path segments are joint and return value is sorted.
func (col *Col) AllIndexesJointPaths() (ret []string) {
	ret = make([]string, 0, 0)
	for _, path := range col.AllIndexes() {
		ret = append(ret, strings.Join(path, INDEX_PATH_SEP))
	}
	sort.Strings(ret)
	return ret
}

// Remove an index.
func (col *Col) Unindex(idxPath []string) (err error) {
	idxName := strings.Join(idxPath, INDEX_PATH_SEP)
	if _, exists := col.indexPaths[idxName]; !exists {
		return fmt.Errorf("Path %v is not indexed", idxPath)
	}
	delete(col.indexPaths, idxName)
	col.hts[idxName].Close()
	delete(col.hts, idxName)
	err = os.RemoveAll(path.Join(col.db.path, col.name, idxName))
	return
}

// Return reference to hash table.
func (col *Col) MultiShardUseHT(jointPath string) *data.HashTable {
	ret := col.hts[jointPath]
	return ret
}

// Place a document lock by memorizing its ID in an internal structure.
func (col *Col) MultiShardLockDoc(id uint64) error {
	if _, locked := col.locked[id]; locked {
		return fmt.Errorf("Document %d is currently locked", id)
	}
	col.locked[id] = struct{}{}
	return nil
}

// Remove a document ID from the internal lock structure.
func (col *Col) MultiShardUnlockDoc(id uint64) {
	delete(col.locked, id)
}

// Insert a new document and immediately place a lock on it.
func (col *Col) MultiShardLockDocAndInsert(id uint64, doc []byte) (err error) {
	if err = col.MultiShardLockDoc(id); err != nil {
		return err
	}
	_, err = col.part.Insert(id, doc)
	return
}

// Retrieve a document by ID, return raw document content.
func (col *Col) BPRead(id uint64) (doc []byte, err error) {
	doc, err = col.part.Read(id)
	return
}

// Retrieve a document by ID and immediately place a lock on it.
func (col *Col) MultiShardLockDocAndRead(id uint64) (doc []byte, err error) {
	if err = col.MultiShardLockDoc(id); err != nil {
		return
	}
	doc, err = col.part.Read(id)
	return
}

// Update a document by ID.
func (col *Col) BPUpdate(id uint64, newDoc []byte) error {
	return col.part.Update(id, newDoc)
}

// Delete a document by ID.
func (col *Col) BPDelete(id uint64) {
	col.part.Delete(id)
}

// Return approximate number of documents in the partition.
func (col *Col) BPApproxDocCount() uint64 {
	return col.part.ApproxDocCount()
}

// Install an index without re-indexing any document.
func (col *Col) BPIndex(idxPath []string) (err error) {
	idxName := strings.Join(idxPath, INDEX_PATH_SEP)
	if _, exists := col.indexPaths[idxName]; exists {
		return fmt.Errorf("Path %v is already indexed", idxPath)
	}
	col.indexPaths[idxName] = idxPath
	idxFileName := path.Join(col.db.path, col.name, idxName)
	col.hts[idxName], err = data.OpenHashTable(idxFileName)
	return
}
