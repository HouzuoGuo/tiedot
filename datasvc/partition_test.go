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
	// Schema version & open & sync
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
	// Insert doc
	if err = client.Call("DataSvc.DocInsert", DocInsertInput{"col1", "doc123", 1, 123}, discard); err == nil || err.Error() != SCHEMA_VERSION_LOW {
		t.Fatal("Did not error")
	}
	if err = client.Call("DataSvc.DocInsert", DocInsertInput{"col1", "doc123", 1, schemaVersion2}, discard); err != nil {
		t.Fatal(err)
	}
	if err = client.Call("DataSvc.DocInsert", DocInsertInput{"col1", "doc098", 2, schemaVersion2}, discard); err != nil {
		t.Fatal(err)
	}
	// Read doc
	var readback string
	if err = client.Call("DataSvc.DocRead", DocReadInput{"col1", 2, 123}, &readback); err == nil || err.Error() != SCHEMA_VERSION_LOW {
		t.Fatal("Did not error")
	}
	if err = client.Call("DataSvc.DocRead", DocReadInput{"col1", 2, schemaVersion2}, &readback); err != nil || readback != "doc098      " {
		t.Fatal(err, readback)
	}
	// Update & readback
	if err = client.Call("DataSvc.DocUpdate", DocUpdateInput{"col1", "01234567890123456789", 2, 123}, discard); err == nil || err.Error() != SCHEMA_VERSION_LOW {
		t.Fatal("Did not error")
	}
	if err = client.Call("DataSvc.DocUpdate", DocUpdateInput{"col1", "01234567890123456789", 2, schemaVersion2}, discard); err != nil {
		t.Fatal(err)
	}
	if err = client.Call("DataSvc.DocRead", DocReadInput{"col1", 2, schemaVersion2}, &readback); err != nil || strings.TrimSpace(readback) != "01234567890123456789" {
		t.Fatal(err, readback)
	}
	if err = client.Call("DataSvc.DocRead", DocReadInput{"col1", 1, schemaVersion2}, &readback); err != nil || strings.TrimSpace(readback) != "doc123" {
		t.Fatal(err, readback)
	}
	// Lock & unlock doc
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
	// Get partition
	var docs map[int]string
	if err = client.Call("DataSvc.DocGetPartition", DocGetPartitionInput{"col1", 0, 1, schemaVersion2}, &docs); err != nil {
		t.Fatal(err)
	}
	if len(docs) != 2 || strings.TrimSpace(docs[1]) != "doc123" || strings.TrimSpace(docs[2]) != "01234567890123456789" {
		t.Fatal(docs)
	}
	// Delete doc
	if err = client.Call("DataSvc.DocDelete", DocDeleteInput{"col1", 2, 123}, discard); err == nil || err.Error() != SCHEMA_VERSION_LOW {
		t.Fatal("Did not error")
	}
	if err = client.Call("DataSvc.DocDelete", DocDeleteInput{"col1", 2, schemaVersion2}, discard); err != nil {
		t.Fatal(err, readback)
	}
	if err = client.Call("DataSvc.DocDelete", DocDeleteInput{"col1", 2, schemaVersion2}, discard); err == nil {
		t.Fatal("Did not error")
	}
	// Clear and close
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
