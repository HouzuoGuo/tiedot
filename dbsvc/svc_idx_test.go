package dbsvc

import (
	"testing"
)

func IdxCrudTest(t *testing.T) {
	var err error
	if err = db.ColCreate("IdxCrudTest"); err != nil {
		t.Fatal(err)
	}
	if err = db.IdxCreate("jdsaiofji", []string{"a", "b"}); err == nil {
		t.Fatal("Did not error")
	}
	if err = db.IdxCreate("IdxCrudTest", []string{"a", "b"}); err != nil {
		t.Fatal(err)
	}
	if err = db.IdxCreate("IdxCrudTest", []string{"c", "d"}); err != nil {
		t.Fatal(err)
	}
	if err = db.IdxCreate("IdxCrudTest", []string{"a", "b"}); err == nil {
		t.Fatal("Did not error")
	}
	if all, err := db.IdxAll("IdxCrudTest"); err != nil || len(all) != 2 || all[0][0] != "a" || all[0][1] != "b" || all[1][0] != "c" || all[1][1] != "d" {
		t.Fatal(all, db.schema)
	}
	if _, err = db.IdxAll("sdf"); err == nil {
		t.Fatal("Did not error")
	}
	if err = db.IdxDrop("jdsaiofji", []string{"a", "b"}); err == nil {
		t.Fatal("Did not error")
	}
	if err = db.IdxDrop("IdxCrudTest", []string{"a", "b"}); err != nil {
		t.Fatal(err)
	}
	if err = db.IdxDrop("IdxCrudTest", []string{"a", "b"}); err == nil {
		t.Fatal("Did not error")
	}
	if all, err := db.IdxAll("IdxCrudTest"); err != nil || len(all) != 1 || all[0][0] != "c" || all[0][1] != "d" {
		t.Fatal(all, db.schema)
	}
	if err = db.ColDrop("IdxCrudTest"); err != nil {
		t.Fatal(err)
	}
}
