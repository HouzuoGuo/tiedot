package data

import (
	"errors"
	"github.com/HouzuoGuo/tiedot/gommap"
	"github.com/bouk/monkey"
	"os"
	"reflect"
	"testing"
)

const tmp = "/tmp/tiedot_test_file"

func TestOpenFlushClose(t *testing.T) {
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
	os.Remove(tmp)
	defer os.Remove(tmp)
	// Open
	tmpFile, err := OpenDataFile(tmp, 1024)
	if err != nil {
		t.Fatalf("Failed to open: %v", err)
		return
	}
	if tmpFile.Used != 0 {
		t.Fatal("Incorrect Used", tmpFile.Used)
	}
	// Write something
	tmpFile.Buf[500] = 1
	tmpFile.Close()

	// Re-open
	tmpFile, err = OpenDataFile(tmp, 1024)
	if err != nil {
		t.Fatalf("Failed to open: %v", err)
	}
	if tmpFile.Used != 501 {
		t.Fatal("Incorrect Used")
	}

	// Write something again
	for i := 750; i < 800; i++ {
		tmpFile.Buf[i] = byte('a')
	}
	tmpFile.Close()

	// Re-open again
	tmpFile, err = OpenDataFile(tmp, 1024)
	if err != nil {
		t.Fatalf("Failed to open: %v", err)
	}
	if tmpFile.Used != 800 {
		t.Fatal("Incorrect Append", tmpFile.Used)
	}
	// Clear the file and test size
	if err = tmpFile.Clear(); err != nil {
		t.Fatal(err)
	}
	if !(len(tmpFile.Buf) == 1024 && tmpFile.Buf[750] == 0 && tmpFile.Growth == 1024 && tmpFile.Size == 1024 && tmpFile.Used == 0) {
		t.Fatal("Did not clear", len(tmpFile.Buf), tmpFile.Growth, tmpFile.Size, tmpFile.Used)
	}
	// Can still write to the buffer?
	tmpFile.Buf[999] = 1
	tmpFile.Close()
}
func TestFileGrow(t *testing.T) {
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
func TestCloseErr(t *testing.T) {
	os.Remove(tmp)
	defer os.Remove(tmp)
	err := "Error close file"
	var d *DataFile
	tmpFile, _ := OpenDataFile(tmp, 1024)
	patch := monkey.PatchInstanceMethod(reflect.TypeOf(d), "Close", func(_ *DataFile) error {
		return errors.New(err)
	})
	if tmpFile.Clear().Error() != err {
		t.Error("Expected error when close file ")
	}
	patch.Unpatch()
}
func TestTruncateError(t *testing.T) {
	os.Remove(tmp)
	defer os.Remove(tmp)
	err := "error truncate"
	tmpFile, _ := OpenDataFile(tmp, 1024)
	patch := monkey.Patch(os.Truncate, func(name string, size int64) error {
		return errors.New(err)
	})
	if tmpFile.Clear().Error() != err {
		t.Error("Expected error when call truncate function")
	}
	patch.Unpatch()
}
func TestFileOpenError(t *testing.T) {
	os.Remove(tmp)
	defer os.Remove(tmp)
	err := "error open file"
	tmpFile, _ := OpenDataFile(tmp, 1024)
	patch := monkey.Patch(os.OpenFile, func(name string, flag int, perm os.FileMode) (*os.File, error) {
		return nil, errors.New(err)
	})
	if tmpFile.Clear().Error() != err {
		t.Error("Expected error when call new open file")
	}
	patch.Unpatch()
}
func TestFillEmptyByteFileError(t *testing.T) {
	os.Remove(tmp)
	defer os.Remove(tmp)
	err := "error fill empty byte new file"
	var f *os.File
	tmpFile, _ := OpenDataFile(tmp, 1024)
	patch := monkey.PatchInstanceMethod(reflect.TypeOf(f), "Seek", func(_ *os.File, offset int64, whence int) (int64, error) {
		return 0, errors.New(err)
	})

	if tmpFile.Clear().Error() != err {
		t.Error("Expected error when fill empty byte new file")
	}
	patch.Unpatch()
}
func TestMapErrorWhanCallClose(t *testing.T) {
	os.Remove(tmp)
	defer os.Remove(tmp)
	err := "error create descriptor to mmap"
	tmpFile, _ := OpenDataFile(tmp, 1024)
	patch := monkey.Patch(gommap.Map, func(f *os.File) (gommap.MMap, error) {
		return nil, errors.New(err)
	})
	if tmpFile.Clear().Error() != err {
		t.Error("Expected error when call  mmap")
	}
	patch.Unpatch()
}
func TestOpenDataFileErrAfterOpen(t *testing.T) {
	os.Remove(tmp)
	defer os.Remove(tmp)
	errMessage := "error after open file"
	patch := monkey.Patch(os.OpenFile, func(name string, flag int, perm os.FileMode) (*os.File, error) {
		return nil, errors.New(errMessage)
	})
	defer patch.Unpatch()

	if _, err := OpenDataFile(tmp, 1024); err.Error() != errMessage {
		t.Error("Expected error when call OpenDataFile")
	}
}
func TestOpenDataSeekErr(t *testing.T) {
	os.Remove(tmp)
	defer os.Remove(tmp)
	errMessage := "error after call Seek"
	var fh *os.File
	patch := monkey.PatchInstanceMethod(reflect.TypeOf(fh), "Seek", func(_ *os.File, offset int64, whence int) (ret int64, err error) {
		return 0, errors.New(errMessage)
	})
	defer patch.Unpatch()
	if _, err := OpenDataFile(tmp, 1024); err.Error() != errMessage {
		t.Error("Expected error when call Seek struct file ")
	}
}
func TestFileSmallerThanGrowth(t *testing.T) {
	os.Remove(tmp)
	defer os.Remove(tmp)
	errMessage := "error not ensure size file"
	var d *DataFile
	var fh *os.File
	patchSeek := monkey.PatchInstanceMethod(reflect.TypeOf(fh), "Seek", func(_ *os.File, offset int64, whence int) (ret int64, err error) {
		return 10, nil
	})
	defer patchSeek.Unpatch()

	patch := monkey.PatchInstanceMethod(reflect.TypeOf(d), "EnsureSize", func(_ *DataFile, more int) (err error) {
		return errors.New(errMessage)
	})
	defer patch.Unpatch()

	if _, err := OpenDataFile(tmp, 1024); err.Error() != errMessage {
		t.Error("Expected error when call EnsureSize function")
	}
}
func TestOverWriteWithZeroErrorFileWrite(t *testing.T) {
	os.Remove(tmp)
	defer os.Remove(tmp)
	errMessage := "error write"
	var fh *os.File
	fd, _ := OpenDataFile(tmp, 1024)
	patchWrite := monkey.PatchInstanceMethod(reflect.TypeOf(fh), "Write", func(_ *os.File, b []byte) (n int, err error) {
		return 0, errors.New(errMessage)
	})
	defer patchWrite.Unpatch()
	fd.Clear()
}
func TestEnsureSizeUnmapErr(t *testing.T) {
	os.Remove(tmp)
	defer os.Remove(tmp)
	errMessage := "error unmap"
	var m *gommap.MMap

	patch := monkey.PatchInstanceMethod(reflect.TypeOf(m), "Unmap", func(_ *gommap.MMap) (err error) {
		return errors.New(errMessage)
	})
	defer patch.Unpatch()

	fd, _ := OpenDataFile(tmp, 1024)
	fd.Used = 2000

	if fd.EnsureSize(0).Error() != errMessage {
		t.Error("Expected error unmap in inner function EnsureSize")
	}
}
func TestEnsureSizeOverwriteWithZeroErr(t *testing.T) {
	os.Remove(tmp)
	defer os.Remove(tmp)
	errMessage := "error Overwrite"
	var fh *os.File
	patch := monkey.PatchInstanceMethod(reflect.TypeOf(fh), "Seek", func(_ *os.File, offset int64, whence int) (ret int64, err error) {
		return 0, errors.New(errMessage)
	})
	defer patch.Unpatch()

	fd, _ := OpenDataFile(tmp, 1024)
	fd.Used = 2000

	if fd.EnsureSize(0).Error() != errMessage {
		t.Error("Expected error `overWriteWithZero` in inner function `EnsureSize`")
	}
}
func TestEnsureSizeMapErr(t *testing.T) {
	os.Remove(tmp)
	defer os.Remove(tmp)
	errMessage := "error map bufer"
	fd, _ := OpenDataFile(tmp, 1024)
	patch := monkey.Patch(gommap.Map, func(f *os.File) (gommap.MMap, error) {
		return nil, errors.New(errMessage)
	})
	defer patch.Unpatch()
	fd.Size = 500
	if fd.EnsureSize(1200).Error() != errMessage {
		t.Error("Expected error `gommap.Map` in inner function `EnsureSize`")
	}
}
