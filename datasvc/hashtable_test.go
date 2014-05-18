package datasvc

import (
	"os"
	"testing"
)

func HTTest(t *testing.T) {
	filename := "/tmp/tiedot_svc_test/ht1"
	os.Remove(filename)
	var schemaVersion1, schemaVersion2, schemaVersion3 int64
	if err = client.Call("DataSvc.SchemaVersion", false, &schemaVersion1); err != nil {
		t.Fatal(err)
	}
	if err = client.Call("DataSvc.HTOpen", HTOpenInput{filename, "ht1"}, discard); err != nil {
		t.Fatal(err)
	}
	if err = client.Call("DataSvc.SchemaVersion", false, &schemaVersion2); err != nil || schemaVersion2 < schemaVersion1 {
		t.Fatal(err, schemaVersion2)
	}
	if err = client.Call("DataSvc.HTSync", "ht1", discard); err != nil {
		t.Fatal(err)
	}
	if err = client.Call("DataSvc.HTSync", "htABCDE", discard); err == nil {
		t.Fatal("Did not error")
	}
	if err = client.Call("DataSvc.HTPut", HTPutInput{"ht1", 100, 200, 123}, discard); err == nil || err.Error() != SCHEMA_VERSION_LOW {
		t.Fatal("Did not error", schemaVersion1, schemaVersion2)
	}
	if err = client.Call("DataSvc.HTPut", HTPutInput{"ht1", 100, 200, schemaVersion2}, discard); err != nil {
		t.Fatal(err)
	}
	if err = client.Call("DataSvc.HTPut", HTPutInput{"ht1", 300, 400, schemaVersion2}, discard); err != nil {
		t.Fatal(err)
	}
	if err = client.Call("DataSvc.HTRemove", HTRemoveInput{"ht1", 300, 400, 123}, discard); err == nil || err.Error() != SCHEMA_VERSION_LOW {
		t.Fatal("Did not error")
	}
	if err = client.Call("DataSvc.HTRemove", HTRemoveInput{"ht1", 300, 400, schemaVersion2}, discard); err != nil {
		t.Fatal(err)
	}
	entries := new(HTGetPartitionOutput)
	if err = client.Call("DataSvc.HTGetPartition", HTGetPartitionInput{"ht1", 0, 10, 123}, entries); err == nil || err.Error() != SCHEMA_VERSION_LOW {
		t.Fatal("Did not error", err)
	}
	if err = client.Call("DataSvc.HTGetPartition", HTGetPartitionInput{"ht1", 1, 0, 123}, entries); err == nil {
		t.Fatal("Did not error")
	}
	if err = client.Call("DataSvc.HTGetPartition", HTGetPartitionInput{"ht1", -1, -1, 123}, entries); err == nil {
		t.Fatal("Did not error")
	}
	if err = client.Call("DataSvc.HTGetPartition", HTGetPartitionInput{"ht1", 0, 1, schemaVersion2}, entries); err != nil {
		t.Fatal(err)
	}
	if !(len(entries.Keys) == 1 && len(entries.Vals) == 1 && entries.Keys[0] == 100 && entries.Vals[0] == 200) {
		t.Fatal(entries)
	}
	var vals []int
	if err = client.Call("DataSvc.HTGet", HTGetInput{"ht1", 100, 0, 123}, &vals); err == nil || err.Error() != SCHEMA_VERSION_LOW {
		t.Fatal("Did not error")
	}
	if err = client.Call("DataSvc.HTGet", HTGetInput{"ht1", 100, 0, schemaVersion2}, &vals); err != nil {
		t.Fatal(err)
	}
	if !(len(vals) == 1 && vals[0] == 200) {
		t.Fatal(vals)
	}
	if err = client.Call("DataSvc.HTClear", "ht1", discard); err != nil {
		t.Fatal(err)
	}
	if err = client.Call("DataSvc.HTClose", "ht1", discard); err != nil {
		t.Fatal(err)
	}
	if err = client.Call("DataSvc.SchemaVersion", false, &schemaVersion3); err != nil || schemaVersion3 < schemaVersion2 {
		t.Fatal(err, schemaVersion3)
	}
	if err = client.Call("DataSvc.HTClose", "ht1", discard); err == nil {
		t.Fatal("Did not error")
	}
}
