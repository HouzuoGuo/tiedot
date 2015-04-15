/*
The package emulates APIs of the older 3.x releases.
When the APIs are used in a way compatible with 3.x releases, it will spawn IPC servers and clients as goroutines,
which behave identical to the embedded DB server of 3.x releases.
It also introduces new APIs for spawning IPC server and child processes, to fully utilise the scalability
improvements brought by the new architecture.
*/

package db

import (
	"github.com/HouzuoGuo/tiedot/data"
	"github.com/HouzuoGuo/tiedot/sharding"
	"github.com/HouzuoGuo/tiedot/tdlog"
	"runtime"
)

// Hosting IPC servers, or clients, or both.
type DB struct {
	dbdir   string
	servers []*sharding.ShardServer
	client  *sharding.RouterClient
}

type Col struct {
	db   *DB
	name string
}

// Emulate 3.x API: run one IPC client, and a number of IPC servers - in goroutines.
func OpenDB(dbdir string) (db *DB, err error) {
	// Create the DB directory if it does not yet exist
	// Initial number of shards = number of GOMAXPROCS
	if err = data.DBNewDir(dbdir, runtime.GOMAXPROCS(0)); err != nil {
		return
	}
	db = &DB{dbdir: dbdir}
	// Run nShards servers
	dbfs, err := data.DBReadDir(dbdir)
	if err != nil {
		return nil, err
	}
	db.servers = make([]*sharding.ShardServer, dbfs.NShards)
	for i := 0; i < dbfs.NShards; i++ {
		db.servers[i] = sharding.NewServer(i, dbdir)
		// Each server starts immediately
		go func(i int) {
			if err := db.servers[i].Run(); err != nil {
				panic(err)
			}
		}(i)
	}
	// ... and one client
	db.client, err = sharding.NewClient(dbdir)
	return
}

// Close all database files. Do not use the DB afterwards!
func (db *DB) Close() error {
	// Use the client to shutdown servers
	db.client.Shutdown()
	return nil
}

// Create a new collection.
func (db *DB) Create(colName string) error {
	return db.client.Create(colName)
}

// Return all collection names.
func (db *DB) AllCols() (ret []string) {
	return db.client.AllCols()
}

// Use the return value to interact with collection.
func (db *DB) Use(colName string) *Col {
	return &Col{db: db, name: colName}
}

// Rename a collection.
func (db *DB) Rename(oldName, newName string) error {
	return db.client.Rename(oldName, newName)
}

// Truncate a collection - delete all documents and clear
func (db *DB) Truncate(colName string) error {
	return db.client.Truncate(colName)
}

// Scrub a collection - fix corrupted documents and de-fragment free space.
func (db *DB) Scrub(colName string) error {
	return db.client.Scrub(colName)
}

// Drop a collection and lose all of its documents and indexes.
func (db *DB) Drop(colName string) error {
	return db.client.Drop(colName)
}

// Copy this database into destination directory (for backup).
func (db *DB) Dump(dest string) error {
	return db.client.Backup(dest)
}

// Do fun for all documents in the collection.
func (col *Col) ForEachDoc(fun func(id uint64, doc []byte) (moveOn bool)) (err error) {
	if err = col.db.client.ForEachDocBytes(col.name, fun); err != nil {
		tdlog.CritNoRepeat("Failed to perform Col.ForEachDoc on collection %s: %v", col.name, err)
	}
	return
}

// Create an index on the path.
func (col *Col) Index(path []string) error {
	return col.db.client.Index(col.name, path)
}

// Return all indexed paths in alphabetical order.
func (col *Col) AllIndexes() (ret [][]string) {
	ret, err := col.db.client.AllIndexes(col.name)
	if err != nil {
		tdlog.CritNoRepeat("Failed to perform Col.AllIndexes on collection %s: %v", col.name, err)
	}
	return
}

// Remove an index.
func (col *Col) Unindex(path []string) error {
	return col.db.client.Unindex(col.name, path)
}

// Return approximate number of documents in the collection.
func (col *Col) ApproxDocCount() (ret uint64) {
	ret, err := col.db.client.ApproxDocCount(col.name)
	if err != nil {
		tdlog.CritNoRepeat("Failed to perform Col.AproxDocCount on collection %s: %v", col.name, err)
	}
	return
}

// Divide the collection into roughly equally sized pages, and do fun on all documents in the specified page.
func (col *Col) ForEachDocInPage(page, total uint64, fun func(id uint64, doc []byte) bool) (err error) {
	docs, err := col.db.client.GetDocPage(col.name, page, total, false)
	if err != nil {
		tdlog.CritNoRepeat("Failed to perform Col.ForEachDocInPage on collection %s: %v", col.name, err)
		return
	}
	for id, docBytes := range docs {
		if !fun(id, docBytes.([]byte)) {
			return nil
		}
	}
	return nil
}

// Main entrance to query processor - evaluate a query and put result into result map (as map keys).
func EvalQuery(q interface{}, src *Col, result *map[uint64]struct{}) (err error) {
	return src.db.client.EvalQuery(q, src.name, result)
}

// Insert a document into the collection.
func (col *Col) Insert(doc map[string]interface{}) (id uint64, err error) {
	return col.db.client.Insert(col.name, doc)
}

// Find and retrieve a document by ID.
func (col *Col) Read(id uint64) (doc map[string]interface{}, err error) {
	return col.db.client.Read(col.name, id)
}

// Update a document.
func (col *Col) Update(id uint64, doc map[string]interface{}) error {
	return col.db.client.Update(col.name, id, doc)
}

// Delete a document.
func (col *Col) Delete(id uint64) error {
	return col.db.client.Delete(col.name, id)
}
