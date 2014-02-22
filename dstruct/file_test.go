/* CommonFile feature test cases. */
package dstruct

import (
	"os"
	"testing"
)

func TestOpenFlushClose(t *testing.T) {
	tmp := "/tmp/tiedot_file_test"
	os.Remove(tmp)
	defer os.Remove(tmp)
	tmpFile, err := OpenFile(tmp, 1000)
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

func TestFindingAppendAndClear(t *testing.T) {
	tmp := "/tmp/tiedot_file_test"
	os.Remove(tmp)
	defer os.Remove(tmp)
	// Open
	tmpFile, err := OpenFile(tmp, 1000)
	if err != nil {
		t.Fatalf("Failed to open: %v", err)
		return
	}
	if tmpFile.UsedSize != 0 {
		t.Fatal("Incorrect UsedSize")
	}
	// Write something
	tmpFile.Buf[500] = 1
	tmpFile.Close()

	// Re-open
	tmpFile, err = OpenFile(tmp, 1000)
	if err != nil {
		t.Fatalf("Failed to open: %v", err)
	}
	if tmpFile.UsedSize != 501 {
		t.Fatal("Incorrect UsedSize")
	}

	// Write something again
	tmpFile.Buf[750] = 1
	tmpFile.Close()

	// Re-open again
	tmpFile, err = OpenFile(tmp, 1000)
	if err != nil {
		t.Fatalf("Failed to open: %v", err)
	}
	if tmpFile.UsedSize != 751 {
		t.Fatalf("Incorrect Append")
	}
	// Clear the file and test size
	tmpFile.Clear()
	if !(len(tmpFile.Buf) == 1000 && tmpFile.Buf[750] == 0 && tmpFile.Growth == 1000 && tmpFile.Size == 1000 && tmpFile.UsedSize == 0) {
		t.Fatal("Did not clear")
	}
	// Can still write to the buffer?
	tmpFile.Buf[999] = 1
	tmpFile.Close()
}

func TestFileGrow(t *testing.T) {
	tmp := "/tmp/tiedot_file_test"
	os.Remove(tmp)
	defer os.Remove(tmp)
	// Open and write something
	tmpFile, err := OpenFile(tmp, 4)
	if err != nil {
		t.Fatalf("Failed to open: %v", err)
		return
	}
	tmpFile.Buf[2] = 1
	tmpFile.UsedSize = 3
	if tmpFile.Size != 4 {
		t.Fatalf("Incorrect Size")
	}
	if tmpFile.CheckSize(1) != true {
		t.Fatalf("Incorrect checksize")
	}
	if tmpFile.CheckSize(2) != false {
		t.Fatalf("Incorrect checksize")
	}
	tmpFile.CheckSizeAndEnsure(8)
	if tmpFile.Size != 12 { // 3 times file growth = 12 bytes
		t.Fatalf("Incorrect Size")
	}
	if tmpFile.UsedSize != 3 { // UsedSize should not change
		t.Fatalf("Incorrect UsedSize")
	}
	if tmpFile.Growth != 4 {
		t.Fatalf("Incorrect Growth")
	}
	// Can write to the new (now larger) region
	tmpFile.Buf[10] = 1
	tmpFile.Buf[11] = 1
	tmpFile.Close()

}
