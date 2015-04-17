package data

import (
	"os"
	"reflect"
	"testing"
)

func TestLoadDBObjects(t *testing.T) {
	dir := "/tmp/tiedot_test_dbdir"
	os.RemoveAll(dir)
	defer os.RemoveAll(dir)

	// Prepare DB directory with collections and indexes
	if err := DBNewDir(dir, 2); err != nil {
		t.Fatal(err)
	}
	dbfs, err := DBReadDir(dir)
	if err != nil {
		t.Fatal(err)
	} else if err := dbfs.CreateCollection("col1"); err != nil {
		t.Fatal(err)
	} else if err := dbfs.CreateCollection("col2"); err != nil {
		t.Fatal(err)
	} else if err := dbfs.CreateIndex("col1", JoinIndexPath([]string{"a", "b"})); err != nil {
		t.Fatal(err)
	} else if err := dbfs.CreateIndex("col1", JoinIndexPath([]string{"c", "d"})); err != nil {
		t.Fatal(err)
	} else if err := dbfs.CreateIndex("col2", JoinIndexPath([]string{"1", "2"})); err != nil {
		t.Fatal(err)
	}

	// Load and verify
	dbo := DBObjectsLoad(dir, -1)
	if names := dbo.GetDBFS().GetCollectionNamesSorted(); !reflect.DeepEqual(names, []string{"col1", "col2"}) {
		t.Fatal(names)
	} else if id1, exists := dbo.GetColIDByName("col1"); !exists || id1 != 0 {
		t.Fatal(id1)
	} else if id2, exists := dbo.GetColIDByName("col2"); !exists || id2 != 3 {
		t.Fatal(id2)
	} else if id3 := dbo.GetIndexIDBySplitPath(id1, []string{"a", "b"}); id3 != 1 {
		t.Fatal(id3, dbo.htIDByPath)
	} else if id4 := dbo.GetIndexIDBySplitPath(id1, []string{"c", "d"}); id4 != 2 {
		t.Fatal(id4)
	} else if id5 := dbo.GetIndexIDBySplitPath(id2, []string{"1", "2"}); id5 != 4 {
		t.Fatal(id5)
	} else if dbo.GetCurrentRev() != 1 {
		t.Fatal(dbo.GetCurrentRev())
	}
	dbo.Reload()
	// Drop a collection and verify
	if dbo.GetCurrentRev() != 2 {
		t.Fatal(dbo.GetCurrentRev())
	} else if err := dbo.GetDBFS().DropCollection("col2"); err != nil {
		t.Fatal(err)
	}
	dbo.ReloadAndSetRev(0)
	if dbo.GetCurrentRev() != 0 {
		t.Fatal(dbo.GetCurrentRev())
	}
	if names := dbo.GetDBFS().GetCollectionNamesSorted(); !reflect.DeepEqual(names, []string{"col1"}) {
		t.Fatal(names)
	} else if id1, exists := dbo.GetColIDByName("col1"); !exists || id1 != 0 {
		t.Fatal(id1)
	} else if id2 := dbo.GetIndexIDBySplitPath(id1, []string{"a", "b"}); id2 != 1 {
		t.Fatal(id2)
	} else if id3 := dbo.GetIndexIDBySplitPath(id1, []string{"c", "d"}); id3 != 2 {
		t.Fatal(id3)
	}
	// Nothing should be loaded when rank == -1
	if len(dbo.hts) > 0 || len(dbo.parts) > 0 {
		t.Fatal(dbo.hts, dbo.parts)
	} else if part, exists := dbo.GetPartByID(0); part != nil || exists {
		t.Fatal("should not have loaded the partition files")
	} else if ht, exists := dbo.GetHashTableByID(2); ht != nil || exists {
		t.Fatal("should not have loaded the partition files")
	}
	// Should load files when rank >= 0
	dbo = DBObjectsLoad(dir, 1)
	if len(dbo.hts) == 0 || len(dbo.parts) == 0 {
		t.Fatal(dbo.hts, dbo.parts)
	} else if part, exists := dbo.GetPartByID(0); part == nil || !exists {
		t.Fatal("should not have loaded the partition files")
	} else if ht, exists := dbo.GetHashTableByID(2); ht == nil || !exists {
		t.Fatal("should not have loaded the partition files")
	}
}
