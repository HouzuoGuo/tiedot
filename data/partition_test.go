package data

import (
	"os"
	"testing"
)

func TestDocCRUD(t *testing.T) {
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
	if err = part.Sync(); err != nil {
		t.Fatal(err)
	}
	if err = part.Close(); err != nil {
		t.Fatal(err)
	}
}
