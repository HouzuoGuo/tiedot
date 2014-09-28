// Binary protocol over IPC - Document management features (client).
package binprot

import (
	"encoding/binary"
	"encoding/json"
	"math/rand"
)

func (client *BinProtClient) docID2RankBytes(id uint64) (rank int, idBytes []byte) {
	rank = int(id % uint64(client.nProcs))
	idBytes = make([]byte, 8)
	binary.LittleEndian.PutUint64(idBytes, id)
	return
}

func (client *BinProtClient) colName2IDBytes(colName string) (colID int32, idBytes []byte) {
	colID = client.colNameLookup[colName]
	idBytes = make([]byte, 4)
	binary.LittleEndian.PutUint32(idBytes, uint32(colID))
	return
}

func (client *BinProtClient) indexDoc(colName string, id uint64, doc map[string]interface{}) error {
	return nil
}

func (client *BinProtClient) unindexDoc(colName string, id uint64, doc map[string]interface{}) error {
	return nil
}

func (client *BinProtClient) Insert(colName string, doc map[string]interface{}) (id uint64, err error) {
	id = uint64(rand.Int63())
	docBytes, err := json.Marshal(doc)
	if err != nil {
		return
	}
	client.opLock.Lock()
	rank, idBytes := client.docID2RankBytes(id)
	_, colIDBytes := client.colName2IDBytes(colName)
	if _, _, err = client.sendCmd(rank, true, C_DOC_INSERT, colIDBytes, idBytes, docBytes); err != nil {
		client.opLock.Lock()
		return
	}
	if _, _, err = client.sendCmd(rank, true, C_DOC_UNLOCK, colIDBytes, idBytes); err != nil {
		client.opLock.Lock()
		return
	}
	client.opLock.Unlock()
	return
}

func (client *BinProtClient) Read(colName string, id uint64) (doc map[string]interface{}, err error) {
	return
}

func (client *BinProtClient) Update(colName string, id uint64, doc map[string]interface{}) error {
	return nil
}

func (client *BinProtClient) Delete(colName string, id uint64) error {
	return nil
}
