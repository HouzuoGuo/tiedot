// Binary protocol over IPC - Document management features (client).
package binprot

func (client *BinProtClient) indexDoc(colName string, id uint64, doc map[string]interface{}) error {
	return nil
}

func (client *BinProtClient) unindexDoc(colName string, id uint64, doc map[string]interface{}) error {
	return nil
}

func (client *BinProtClient) Insert(colName string, doc map[string]interface{}) (id uint64, err error) {
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
