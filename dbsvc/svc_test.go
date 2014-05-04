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
	NUM_SRVS      = 2
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
	time.Sleep(50 * time.Millisecond)
	if db, err = NewDBSvc(NUM_SRVS, TEST_SRV_DIR, TEST_DATA_DIR); err != nil {
		t.Fatal(err)
	}
	// Run test sequence
	IDTest(t)
	MgmtTest(t)
	ColCrudTest(t)
	// Shutdown and cleanup
	if err = db.Shutdown(); err != nil {
		t.Fatal(err)
	}
}

func IDTest(t *testing.T) {
	if mkIndexUID("a", []string{"b", "c"}) != "a!b!c" {
		t.Fatal()
	}
	if colName, idxPath := destructIndexUID("a!b!c_d"); colName != "a" || idxPath[0] != "b" || idxPath[1] != "c_d" {
		t.Fatal(colName, idxPath)
	}
	if db.mkColDirName("My_Stuff_1") != "My_Stuff_1_2" {
		t.Fatal("Wrong name")
	}
	if _, _, err := db.destructColDirName("_2"); err == nil {
		t.Fatal("Did not error")
	}
	if _, _, err := db.destructColDirName("My_Stuff_"); err == nil {
		t.Fatal("Did not error")
	}
	if _, _, err := db.destructColDirName("My_Stuff_A"); err == nil {
		t.Fatal("Did not error")
	}
	if name, parts, err := db.destructColDirName("abc_2"); err != nil || name != "abc" || parts != 2 {
		t.Fatal(name, parts, err)
	}
	if name, parts, err := db.destructColDirName("My_Collection_2"); err != nil || name != "My_Collection" || parts != 2 {
		t.Fatal(name, parts, err)
	}
}
