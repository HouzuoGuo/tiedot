package srv

import (
	"github.com/HouzuoGuo/tiedot/dsserver/colpart"
	"github.com/HouzuoGuo/tiedot/dsserver/dstruct"
)

// Struct for exchanging document insert/update/delete operations
type DocCRUD struct {
	name string                 // Collection name
	id   uint64                 // Document ID
	doc  map[string]interface{} // Document content
}

// Server state
type RpcServer struct {
	Rank, TotalRank int                                      // Rank of current process; total number of processes
	ColParts        map[string]*colpart.Partition            // Collection name -> partition
	Htables         map[string]*dstruct.HashTable // Collection name -> index name -> hash table
}
