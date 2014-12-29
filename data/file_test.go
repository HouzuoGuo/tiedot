package data

import (
	"os"
	"testing"
)

func TestOpenFlushClose(t *testing.T) {
	tmp := "/tmp/tiedot_test_file"
	os.Remove(tmp)
	defer os.Remove(tmp)
	tmpFile, err := OpenDataFile(tmp, 999)
	if err != nil {
		t.Fatalf("Failed to open: %v", err)
		return
	}
	defer tmpFile.Close()
	if tmpFile.Path != tmp {
		t.Fatal("Name not set")
	}
	if tmpFile.Used != 0 {
		t.Fatal("Incorrect Used")
	}
	if tmpFile.Growth != 999 {
		t.Fatal("Growth not set")
	}
	if tmpFile.Fh == nil || tmpFile.Buf == nil {
		t.Fatal("Not mmapped")
	}
	if err := tmpFile.Close(); err != nil {
		t.Fatalf("Failed to close: %v", err)
	}
}

func TestFindingAppendAndClear(t *testing.T) {
	tmp := "/tmp/tiedot_test_file"
	os.Remove(tmp)
	defer os.Remove(tmp)
	// Open
	tmpFile, err := OpenDataFile(tmp, 500)
	if err != nil {
		t.Fatalf("Failed to open: %v", err)
		return
	}
	if tmpFile.Used != 0 {
		t.Fatal("Incorrect Used", tmpFile.Used)
	}
	// Write something
	tmpFile.Buf[100] = 1
	tmpFile.Close()

	// Re-open
	tmpFile, err = OpenDataFile(tmp, 500)
	if err != nil {
		t.Fatalf("Failed to open: %v", err)
	}
	if tmpFile.Used != 101 {
		t.Fatal("Incorrect Used")
	}
	// Write again and test used-size calculation
	for i := 150; i < 179; i++ {
		tmpFile.Buf[i] = byte('a')
	}
	tmpFile.Close()
	tmpFile, err = OpenDataFile(tmp, 500)
	if err != nil {
		t.Fatalf("Failed to open: %v", err)
	}
	if tmpFile.Used != 179 {
		t.Fatal("Incorrect Append", tmpFile.Used)
	}
	// Do it once more
	for i := 263; i < 367; i++ {
		tmpFile.Buf[i] = byte('a')
	}
	tmpFile.Close()
	tmpFile, err = OpenDataFile(tmp, 500)
	if err != nil {
		t.Fatalf("Failed to open: %v", err)
	}
	if tmpFile.Used != 367 {
		t.Fatal("Incorrect Append", tmpFile.Used)
	}
	// Clear the file and test size
	if err = tmpFile.Clear(); err != nil {
		t.Fatal(err)
	}
	if !(len(tmpFile.Buf) == 500 && tmpFile.Buf[367] == 0 && tmpFile.Growth == 500 && tmpFile.Size == 500 && tmpFile.Used == 0) {
		t.Fatal("Did not clear", len(tmpFile.Buf), tmpFile.Growth, tmpFile.Size, tmpFile.Used)
	}
	// Can still write to the buffer?
	tmpFile.Buf[499] = 1
	if err := tmpFile.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestFileGrow(t *testing.T) {
	tmp := "/tmp/tiedot_test_file"
	os.Remove(tmp)
	defer os.Remove(tmp)
	// Open and write something
	tmpFile, err := OpenDataFile(tmp, 4)
	if err != nil {
		t.Fatalf("Failed to open: %v", err)
		return
	}
	tmpFile.Buf[2] = 1
	tmpFile.Used = 3
	if tmpFile.Size != 4 {
		t.Fatal("Incorrect Size", tmpFile.Size)
	}
	tmpFile.EnsureSize(8)
	if tmpFile.Size != 12 { // 3 times file growth = 12 bytes
		t.Fatalf("Incorrect Size")
	}
	if tmpFile.Used != 3 { // Used should not change
		t.Fatalf("Incorrect Used")
	}
	if len(tmpFile.Buf) != 12 {
		t.Fatal("Did not remap")
	}
	if tmpFile.Growth != 4 {
		t.Fatalf("Incorrect Growth")
	}
	// Can write to the new (now larger) region
	tmpFile.Buf[10] = 1
	tmpFile.Buf[11] = 1
	tmpFile.Close()
}
