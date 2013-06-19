package db

import (
	"os"
	"testing"
)

func TestCRUD(t *testing.T) {
	tmp := "/tmp/tiedot_db_test"
	os.RemoveAll(tmp)
	defer os.RemoveAll(tmp)
	db, err := OpenDB(tmp)
	if err != nil {
		t.Errorf("Failed to open: %v", err)
		return
	}
	// create
	if err := db.Create("a"); err != nil {
		t.Errorf("Failed to create: %v", err)
	}
	if err := db.Create("b"); err != nil {
		t.Errorf("Failed to create: %v", err)
	}
	if db.Use("a") == nil {
		t.Errorf("a doesn't exist?!")
	}
	if db.Use("b") == nil {
		t.Errorf("b doesn't exist?!")
	}
	// use
	if err := db.Use("a").BackupAndSaveConf(); err != nil {
		t.Errorf("Use does not work")
	}
	// rename
	if err := db.Rename("a", "c"); err != nil {
		t.Error(err)
	}
	if _, nope := db.StrCol["a"]; nope {
		t.Error("a still exists after it is renamed")
	}
	if _, ok := db.StrCol["c"]; !ok {
		t.Error("c does not exist")
	}
	if err := db.Use("c").BackupAndSaveConf(); err != nil {
		t.Errorf("c is unusable")
	}
	// drop
	if err := db.Drop("c"); err != nil {
		t.Error(err)
	}
	if _, nope := db.StrCol["c"]; nope {
		t.Error("c still exists after it is deleted")
	}
	db.Close()
}
