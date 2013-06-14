package file

import (
	"os"
	"testing"
)

func TestOpenClose(t *testing.T) {
	tmp := "/tmp/tiedot_file_test"
	defer os.Remove(tmp)
	tmpFile, err := Open(tmp, 1000)
	if err != nil {
		t.Errorf("Failed to open: %v", err)
		return
	}
	if tmpFile.Name != tmp {
		t.Error("Name not set")
	}
	if tmpFile.Append != 0 {
		t.Error("Incorrect Append")
	}
	if tmpFile.Growth != 1000 {
		t.Error("Growth not set")
	}
	if tmpFile.Fh == nil || tmpFile.Buf == nil {
		t.Error("Not mmapped")
	}
	if err := tmpFile.Close(); err != nil {
		t.Errorf("Failed to close: %v", err)
	}
}

func TestFindingAppend(t *testing.T) {
	tmp := "/tmp/tiedot_file_test"
	defer os.Remove(tmp)
	// Open
	tmpFile, err := Open(tmp, 1000)
	if err != nil {
		t.Errorf("Failed to open: %v", err)
		return
	}
	if tmpFile.Append != 0 {
		t.Error("Incorrect Append")
	}
	// Write something
	tmpFile.Buf[0] = 0
	tmpFile.Buf[1] = 1
	tmpFile.Buf[2] = 2
	tmpFile.Close()

	// Re-open
	tmpFile, err = Open(tmp, 1000)
	if err != nil {
		t.Errorf("Failed to open: %v", err)
	}
	if tmpFile.Append != 3 {
		t.Error("Incorrect Append")
	}

	// Write something again
	tmpFile.Buf[3] = 3
	tmpFile.Close()

	// Re-open again
	tmpFile, err = Open(tmp, 1000)
	if err != nil {
		t.Errorf("Failed to open: %v", err)
	}
	if tmpFile.Append != 4 {
		t.Error("Incorrect Append")
	}
	tmpFile.Close()
}
