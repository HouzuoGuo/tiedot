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
	if err = client.Call("DataSvc.PartOpen", PartOpenInput{colPath, htPath, "col1"}, discard); err != nil {
		t.Fatal(err)
	}
	if err = client.Call("DataSvc.PartSync", "col1", discard); err != nil {
		t.Fatal(err)
	}
	if err = client.Call("DataSvc.PartSync", "colABCD", discard); err == nil {
		t.Fatal("Did not error")
	}
	if err = client.Call("DataSvc.DocInsert", DocInsertInput{"col1", "doc123", 1}, discard); err != nil {
		t.Fatal(err)
	}
	if err = client.Call("DataSvc.DocInsert", DocInsertInput{"col1", "doc098", 2}, discard); err != nil {
		t.Fatal(err)
	}
	var readback string
	if err = client.Call("DataSvc.DocRead", DocReadInput{"col1", 2}, &readback); err != nil || readback != "doc098      " {
		t.Fatal(err, readback)
	}
	if err = client.Call("DataSvc.DocUpdate", DocUpdateInput{"col1", "01234567890123456789", 2}, discard); err != nil {
		t.Fatal(err)
	}
	if err = client.Call("DataSvc.DocRead", DocReadInput{"col1", 2}, &readback); err != nil || strings.TrimSpace(readback) != "01234567890123456789" {
		t.Fatal(err, readback)
	}
	if err = client.Call("DataSvc.DocDelete", DocDeleteInput{"col1", 2}, discard); err != nil {
		t.Fatal(err, readback)
	}
	if err = client.Call("DataSvc.DocDelete", DocDeleteInput{"col1", 2}, discard); err == nil {
		t.Fatal("Did not error")
	}
	if err = client.Call("DataSvc.PartClose", "col1", discard); err != nil {
		t.Fatal(err)
	}
	if err = client.Call("DataSvc.PartClose", "col1", discard); err == nil {
		t.Fatal("Did not error")
	}

}
