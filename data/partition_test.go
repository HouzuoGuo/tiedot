package data

import (
	"os"
	"testing"
)

func TestPartitionDocCRUD(t *testing.T) {
	colPath := "/tmp/tiedot_test_col"
	htPath := "/tmp/tiedot_test_ht"
	os.Remove(colPath)
	os.Remove(htPath)
	defer os.Remove(colPath)
	defer os.Remove(htPath)
	part, err := OpenPartition(colPath, htPath)
	if err != nil {
		t.Fatal(err)
	}
	// Insert & read
	if _, err = part.Insert(1, []byte("1")); err != nil {
		t.Fatal(err)
	}
	if _, err = part.Insert(2, []byte("2")); err != nil {
		t.Fatal(err)
	}
	if readback, err := part.Read(1); err != nil || string(readback) != "1 " {
		t.Fatal(err, readback)
	}
	if readback, err := part.Read(2); err != nil || string(readback) != "2 " {
		t.Fatal(err, readback)
	}
	// Update & read
	if err = part.Update(1, []byte("abcdef")); err != nil {
		t.Fatal(err)
	}
	if err = part.Update(1234, []byte("abcdef")); err == nil {
		t.Fatal("Did not error")
	}
	if readback, err := part.Read(1); err != nil || string(readback) != "abcdef      " {
		t.Fatal(err, readback)
	}
	// Delete & read
	if err = part.Delete(1); err != nil {
		t.Fatal(err)
	}
	if _, err = part.Read(1); err == nil {
		t.Fatal("Did not error")
	}
	if err = part.Delete(123); err == nil {
		t.Fatal("Did not error")
	}
	// Lock & unlock
	if err = part.LockUpdate(123); err != nil {
		t.Fatal(err)
	}
	if err = part.LockUpdate(123); err == nil {
		t.Fatal("Did not error")
	}
	part.UnlockUpdate(123)
	if err = part.LockUpdate(123); err != nil {
		t.Fatal(err)
	}
	// Foreach
	part.ForEachDoc(0, 1, func(id int, doc []byte) bool {
		if id != 2 || string(doc) != "2 " {
			t.Fatal("ID 2 should be the only remaining document")
		}
		return true
	})
	// Finish up
	if err = part.Clear(); err != nil {
		t.Fatal(err)
	}
	if err = part.Sync(); err != nil {
		t.Fatal(err)
	}
	if err = part.Close(); err != nil {
		t.Fatal(err)
	}
}
