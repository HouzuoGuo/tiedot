package datasvc

import (
	"os"
	"strings"
	"testing"
)

func PartitionTest(t *testing.T) {
	colPath := "/tmp/tiedot_test_col"
	htPath := "/tmp/tiedot_test_ht"
	os.Remove(colPath)
	os.Remove(htPath)
	defer os.Remove(colPath)
	defer os.Remove(htPath)
	var schemaVersion1, schemaVersion2, schemaVersion3 int64
	if err = client.Call("DataSvc.SchemaVersion", false, &schemaVersion1); err != nil {
		t.Fatal(err)
	}
	if err = client.Call("DataSvc.PartOpen", PartOpenInput{colPath, htPath, "col1"}, discard); err != nil {
		t.Fatal(err)
	}
	if err = client.Call("DataSvc.SchemaVersion", false, &schemaVersion2); err != nil || schemaVersion2 < schemaVersion1 {
		t.Fatal(err, schemaVersion2)
	}
	if err = client.Call("DataSvc.PartSync", "col1", discard); err != nil {
		t.Fatal(err)
	}
	if err = client.Call("DataSvc.PartSync", "colABCD", discard); err == nil {
		t.Fatal("Did not error")
	}
	if err = client.Call("DataSvc.DocInsert", DocInsertInput{"col1", "doc123", 1, 123}, discard); err == nil || err.Error() != SCHEMA_VERSION_LOW {
		t.Fatal("Did not error")
	}
	if err = client.Call("DataSvc.DocInsert", DocInsertInput{"col1", "doc123", 1, schemaVersion2}, discard); err != nil {
		t.Fatal(err)
	}
	if err = client.Call("DataSvc.DocInsert", DocInsertInput{"col1", "doc098", 2, schemaVersion2}, discard); err != nil {
		t.Fatal(err)
	}
	var readback string
	if err = client.Call("DataSvc.DocRead", DocReadInput{"col1", 2, 123}, &readback); err == nil || err.Error() != SCHEMA_VERSION_LOW {
		t.Fatal("Did not error")
	}
	if err = client.Call("DataSvc.DocRead", DocReadInput{"col1", 2, schemaVersion2}, &readback); err != nil || readback != "doc098      " {
		t.Fatal(err, readback)
	}
	if err = client.Call("DataSvc.DocUpdate", DocUpdateInput{"col1", "01234567890123456789", 2, 123}, discard); err == nil || err.Error() != SCHEMA_VERSION_LOW {
		t.Fatal("Did not error")
	}
	if err = client.Call("DataSvc.DocUpdate", DocUpdateInput{"col1", "01234567890123456789", 2, schemaVersion2}, discard); err != nil {
		t.Fatal(err)
	}
	if err = client.Call("DataSvc.DocRead", DocReadInput{"col1", 2, schemaVersion2}, &readback); err != nil || strings.TrimSpace(readback) != "01234567890123456789" {
		t.Fatal(err, readback)
	}
	if err = client.Call("DataSvc.DocLockUpdate", DocLockUpdateInput{"col1", 2, 123}, discard); err == nil || err.Error() != SCHEMA_VERSION_LOW {
		t.Fatal("Did not error")
	}
	if err = client.Call("DataSvc.DocLockUpdate", DocLockUpdateInput{"col1", 2, schemaVersion2}, discard); err != nil {
		t.Fatal(err)
	}
	if err = client.Call("DataSvc.DocLockUpdate", DocLockUpdateInput{"col1", 2, schemaVersion2}, discard); err == nil {
		t.Fatal("Did not error")
	}
	if err = client.Call("DataSvc.DocUnlockUpdate", DocUnlockUpdateInput{"col1", 2, 123}, discard); err == nil || err.Error() != SCHEMA_VERSION_LOW {
		t.Fatal("Did not error")
	}
	if err = client.Call("DataSvc.DocUnlockUpdate", DocUnlockUpdateInput{"col1", 2, schemaVersion2}, discard); err != nil {
		t.Fatal(err)
	}
	if err = client.Call("DataSvc.DocDelete", DocDeleteInput{"col1", 2, 123}, discard); err == nil || err.Error() != SCHEMA_VERSION_LOW {
		t.Fatal("Did not error")
	}
	if err = client.Call("DataSvc.DocDelete", DocDeleteInput{"col1", 2, schemaVersion2}, discard); err != nil {
		t.Fatal(err, readback)
	}
	if err = client.Call("DataSvc.DocDelete", DocDeleteInput{"col1", 2, schemaVersion2}, discard); err == nil {
		t.Fatal("Did not error")
	}
	if err = client.Call("DataSvc.PartClear", "col1", discard); err != nil {
		t.Fatal(err)
	}
	if err = client.Call("DataSvc.PartClose", "col1", discard); err != nil {
		t.Fatal(err)
	}
	if err = client.Call("DataSvc.SchemaVersion", false, &schemaVersion3); err != nil || schemaVersion3 < schemaVersion2 {
		t.Fatal(err, schemaVersion3)
	}
	if err = client.Call("DataSvc.PartClose", "col1", discard); err == nil {
		t.Fatal("Did not error")
	}
}
