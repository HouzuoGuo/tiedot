package dbsvc

import "testing"

func ColCrudTest(t *testing.T) {
	var err error
	if err = db.ColCreate("My_Collection"); err != nil {
		t.Fatal(err)
	}
	if err = db.ColCreate("My_Collection"); err == nil {
		t.Fatal("Did not error")
	}
	if allNames, err := db.ColAll(); err != nil || len(allNames) != 1 || allNames[0] != "My_Collection" {
		t.Fatal(err, allNames, db.schema)
	}
	if err = db.ColCreate("My_Collection_2"); err != nil {
		t.Fatal(err)
	}
	if allNames, err := db.ColAll(); err != nil || len(allNames) != 2 || allNames[0] != "My_Collection" || allNames[1] != "My_Collection_2" {
		t.Fatal(err, allNames, db.schema)
	}
	if err = db.ColRename("My_Collection_2", "My_Collection_2"); err == nil {
		t.Fatal("Did not error")
	}
	if err = db.ColRename("aaaa", "My_Collection_3"); err == nil {
		t.Fatal("Did not error")
	}
	if err = db.ColRename("My_Collection_2", "My_Collection"); err == nil {
		t.Fatal("Did not error")
	}
	if err = db.ColRename("My_Collection_2", "My_Collection_3"); err != nil {
		t.Fatal(err)
	}
	if allNames, err := db.ColAll(); err != nil || len(allNames) != 2 || allNames[0] != "My_Collection" || allNames[1] != "My_Collection_3" {
		t.Fatal(err, allNames, db.schema)
	}
	if err = db.ColTruncate("abc"); err == nil {
		t.Fatal("Did not error")
	}
	if err = db.ColTruncate("My_Collection"); err != nil {
		t.Fatal(err)
	}
	if err = db.ColTruncate("My_Collection_3"); err != nil {
		t.Fatal(err)
	}
	if allNames, err := db.ColAll(); err != nil || len(allNames) != 2 || allNames[0] != "My_Collection" || allNames[1] != "My_Collection_3" {
		t.Fatal(err, allNames, db.schema)
	}
	if err = db.ColDrop("aaa"); err == nil {
		t.Fatal("Did not error")
	}
	if err = db.ColDrop("My_Collection"); err != nil {
		t.Fatal(err)
	}
	if allNames, err := db.ColAll(); err != nil || len(allNames) != 1 || allNames[0] != "My_Collection_3" {
		t.Fatal(err, allNames, db.schema)
	}
	if err = db.ColDrop("My_Collection_3"); err != nil {
		t.Fatal(err)
	}
	if allNames, err := db.ColAll(); err != nil || len(allNames) != 0 {
		t.Fatal(err, allNames, db.schema)
	}
}
