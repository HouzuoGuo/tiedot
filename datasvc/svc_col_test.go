package datasvc

import (
	"os"
	"strings"
	"testing"
)

func ColTest(t *testing.T) {
	filename := "/tmp/tiedot_svc_test/col1"
	os.Remove(filename)
	if err = client.Call("DataSvc.ColOpen", ColOpenInput{filename, "col1"}, discard); err != nil {
		t.Fatal(err)
	}
	if err = client.Call("DataSvc.ColSync", "col1", discard); err != nil {
		t.Fatal(err)
	}
	if err = client.Call("DataSvc.ColSync", "colABCD", discard); err == nil {
		t.Fatal("Did not error")
	}
	var docID int
	if err = client.Call("DataSvc.DocInsert", DocInsertInput{"col1", "doc123"}, &docID); err != nil {
		t.Fatal(err)
	}
	if err = client.Call("DataSvc.DocInsert", DocInsertInput{"col1", "doc098"}, &docID); err != nil || docID == 0 {
		t.Fatal(err, docID)
	}
	var readback string
	if err = client.Call("DataSvc.DocRead", DocReadInput{"col1", docID}, &readback); err != nil || readback != "doc098      " {
		t.Fatal(err, readback)
	}
	var newDocID int
	if err = client.Call("DataSvc.DocUpdate", DocUpdateInput{"col1", "01234567890123456789", docID}, &newDocID); err != nil || newDocID == docID {
		t.Fatal(err, docID)
	}
	if err = client.Call("DataSvc.DocRead", DocReadInput{"col1", newDocID}, &readback); err != nil || strings.TrimSpace(readback) != "01234567890123456789" {
		t.Fatal(err, readback)
	}
	if err = client.Call("DataSvc.DocDelete", DocDeleteInput{"col1", newDocID}, discard); err != nil {
		t.Fatal(err, readback)
	}
	if err = client.Call("DataSvc.DocDelete", DocDeleteInput{"col1", newDocID}, discard); err == nil {
		t.Fatal("Did not error")
	}
	if err = client.Call("DataSvc.ColClose", "col1", discard); err != nil {
		t.Fatal(err)
	}
	if err = client.Call("DataSvc.ColClose", "col1", discard); err == nil {
		t.Fatal("Did not error")
	}

}
