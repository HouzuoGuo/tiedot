package file

import (
	"os"
	"testing"
)

func TestOpenFlushClose(t *testing.T) {
	tmp := "/tmp/tiedot_file_test"
	os.Remove(tmp)
	defer os.Remove(tmp)
	tmpFile, err := Open(tmp, 1000)
	if err != nil {
		t.Fatalf("Failed to open: %v", err)
		return
	}
	defer tmpFile.Close()
	if tmpFile.Name != tmp {
		t.Fatal("Name not set")
	}
	if tmpFile.UsedSize != 0 {
		t.Fatal("Incorrect UsedSize")
	}
	if tmpFile.Growth != 1000 {
		t.Fatal("Growth not set")
	}
	if tmpFile.Fh == nil || tmpFile.Buf == nil {
		t.Fatal("Not mmapped")
	}
	if err := tmpFile.Flush(); err != nil {
		t.Fatalf("Failed to flush: %v", err)
	}
	if err := tmpFile.Close(); err != nil {
		t.Fatalf("Failed to close: %v", err)
	}
}

func TestFindingAppend(t *testing.T) {
	tmp := "/tmp/tiedot_file_test"
	os.Remove(tmp)
	defer os.Remove(tmp)
	// Open
	tmpFile, err := Open(tmp, 1000)
	if err != nil {
		t.Fatalf("Failed to open: %v", err)
		return
	}
	if tmpFile.UsedSize != 0 {
		t.Fatal("Incorrect UsedSize")
	}
	// Write something
	tmpFile.Buf[0] = 0
	tmpFile.Buf[1] = 1
	tmpFile.Buf[2] = 2
	tmpFile.Close()

	// Re-open
	tmpFile, err = Open(tmp, 1000)
	if err != nil {
		t.Fatalf("Failed to open: %v", err)
	}
	if tmpFile.UsedSize != 3 {
		t.Fatal("Incorrect UsedSize")
	}

	// Write something again
	tmpFile.Buf[3] = 3
	tmpFile.Close()

	// Re-open again
	tmpFile, err = Open(tmp, 1000)
	if err != nil {
		t.Fatalf("Failed to open: %v", err)
	}
	if tmpFile.UsedSize != 4 {
		t.Fatalf("Incorrect Append")
	}
	tmpFile.Close()
}
