package dbsvc

import (
	"github.com/HouzuoGuo/tiedot/datasvc"
	"os"
	"testing"
	"time"
)

var err error
var srv []*datasvc.DataSvc = make([]*datasvc.DataSvc, NUM_SRVS)
var db *DBSvc

const (
	TEST_SRV_DIR  = "/tmp/tiedot_dc_test"
	TEST_DATA_DIR = "/tmp/tiedot_dc_test_data"
	NUM_SRVS      = 4
)

func TestSequence(t *testing.T) {
	os.RemoveAll(TEST_DATA_DIR)
	os.RemoveAll(TEST_SRV_DIR)
	defer os.RemoveAll(TEST_SRV_DIR)
	defer os.RemoveAll(TEST_DATA_DIR)
	os.MkdirAll(TEST_SRV_DIR, 0700)
	// Prepare 4 data servers
	for i := 0; i < NUM_SRVS; i++ {
		srv[i] = datasvc.NewDataSvc(TEST_SRV_DIR, i)
		go func(i int) {
			if err = srv[i].Serve(); err != nil {
				panic(err)
			}
		}(i)
	}
	time.Sleep(100 * time.Millisecond)
	if db, err = NewDBSvc(NUM_SRVS, TEST_SRV_DIR, TEST_DATA_DIR); err != nil {
		t.Fatal(err)
	}
	// Run test sequence
	SchemaTest(t)
	// Shutdown and cleanup
	if err = db.Shutdown(); err != nil {
		t.Fatal(err)
	}
}
