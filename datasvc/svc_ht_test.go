package datasvc

import (
	"os"
	"testing"
)

func HTTest(t *testing.T) {
	filename := "/tmp/tiedot_svc_test/ht1"
	os.Remove(filename)
	if err = client.Call("DataSvc.HTOpen", HTOpenInput{filename, "ht1"}, discard); err != nil {
		t.Fatal(err)
	}
	if err = client.Call("DataSvc.HTSync", "ht1", discard); err != nil {
		t.Fatal(err)
	}
	if err = client.Call("DataSvc.HTSync", "htABCDE", discard); err == nil {
		t.Fatal("Did not error")
	}
	if err = client.Call("DataSvc.HTPut", HTPutInput{"ht1", 100, 200}, discard); err != nil {
		t.Fatal(err)
	}
	if err = client.Call("DataSvc.HTPut", HTPutInput{"ht1", 300, 400}, discard); err != nil {
		t.Fatal(err)
	}
	if err = client.Call("DataSvc.HTRemove", HTRemoveInput{"ht1", 300, 400}, discard); err != nil {
		t.Fatal(err)
	}
	allEntries := new(HTAllEntriesOutput)
	if err = client.Call("DataSvc.HTAllEntries", HTAllEntriesInput{"ht1", 0}, allEntries); err != nil {
		t.Fatal(err)
	}
	if !(len(allEntries.Keys) == 1 && len(allEntries.Vals) == 1 && allEntries.Keys[0] == 100 && allEntries.Vals[0] == 200) {
		t.Fatal(allEntries)
	}
	var vals []int
	if err = client.Call("DataSvc.HTGet", HTGetInput{"ht1", 100, 0}, &vals); err != nil {
		t.Fatal(err)
	}
	if !(len(vals) == 1 && vals[0] == 200) {
		t.Fatal(vals)
	}
	if err = client.Call("DataSvc.HTClose", "ht1", discard); err != nil {
		t.Fatal(err)
	}
	if err = client.Call("DataSvc.HTClose", "ht1", discard); err == nil {
		t.Fatal("Did not error")
	}
}
