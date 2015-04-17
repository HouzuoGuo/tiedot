/*
Provide common IO features for data structure files based on memory-mapped buffer:
- Create file with an initial capacity.
- Grow file when more capacity is demanded (growth size is constant).
- Keep track of amount of space occupied by data ("Used Size").
- Access binary, text, or mixed content at random location.
*/
package data

import (
	"github.com/HouzuoGuo/tiedot/gommap"
	"github.com/HouzuoGuo/tiedot/tdlog"
	"os"
)

// Data structure file contains random-accessible binary, text, or mixed content, and keeps track of used size.
type DataFile struct {
	Path               string
	Size, Used, Growth uint64
	Fh                 *os.File
	Buf                gommap.MMap
}

/*
Return true if the buffer slice begins with 32 consecutive bytes of zero.
Used for calculating the Used Size of a data structure file.
IMPORTANT: data structure implementation must re-implement the Used Size calculation if there is a chance for 32
consecutive bytes of 0 to appear in middle of the used portion of the data file.
*/
func BufLooksEmpty(buf gommap.MMap) bool {
	upTo := 32
	if upTo >= len(buf) {
		upTo = len(buf) - 1
	}
	for i := 0; i < upTo; i++ {
		if buf[i] != 0 {
			return false
		}
	}
	return true
}

// Open/create a data file with an initial capacity and size growth constant.
func OpenDataFile(path string, growth uint64) (file *DataFile, err error) {
	file = &DataFile{Path: path, Growth: growth}
	if file.Fh, err = os.OpenFile(file.Path, os.O_CREATE|os.O_RDWR, 0600); err != nil {
		return
	}
	var size int64
	if size, err = file.Fh.Seek(0, os.SEEK_END); err != nil {
		return
	}
	// Grow the file if the initial size is smaller than specified
	if file.Size = uint64(size); file.Size < file.Growth {
		if err = file.EnsureSize(file.Growth); err != nil {
			return
		}
	}
	if file.Buf == nil {
		file.Buf, err = gommap.Map(file.Fh)
	}
	defer tdlog.Infof("%s opened: %d of %d bytes in-use", file.Path, file.Used, file.Size)
	// Bi-sect file buffer to find out how much space is in-use
	for low, mid, high := uint64(0), file.Size/2, file.Size; ; {
		switch {
		case high-mid == 1:
			if BufLooksEmpty(file.Buf[mid:]) {
				if mid > 0 && BufLooksEmpty(file.Buf[mid-1:]) {
					file.Used = mid - 1
				} else {
					file.Used = mid
				}
				return
			}
			file.Used = high
			return
		case BufLooksEmpty(file.Buf[mid:]):
			high = mid
			mid = low + (mid-low)/2
		default:
			low = mid
			mid = mid + (high-mid)/2
		}
	}
}

// Fill up portion of a file with 0s.
func (file *DataFile) overwriteWithZero(from uint64, size uint64) (err error) {
	if _, err = file.Fh.Seek(int64(from), os.SEEK_SET); err != nil {
		return
	}
	zeroSize := uint64(1048576 * 8) // Fill 8 MB at a time
	zero := make([]byte, zeroSize)
	for i := uint64(0); i < size; i += zeroSize {
		var zeroSlice []byte
		if i+zeroSize > size {
			zeroSlice = zero[0 : size-i]
		} else {
			zeroSlice = zero
		}
		if _, err = file.Fh.Write(zeroSlice); err != nil {
			return
		}
	}
	return file.Fh.Sync()
}

// Make sure there is enough room for some more data.
func (file *DataFile) EnsureSize(more uint64) (err error) {
	if file.Used+more <= file.Size {
		return
	} else if file.Buf != nil {
		if err = file.Buf.Unmap(); err != nil {
			return
		}
	}
	if err = file.overwriteWithZero(file.Size, file.Growth); err != nil {
		return
	} else if file.Buf, err = gommap.Map(file.Fh); err != nil {
		return
	}
	file.Size += file.Growth
	tdlog.Infof("%s grown: %d -> %d bytes (%d bytes in-use)", file.Path, file.Size-file.Growth, file.Size, file.Used)
	return file.EnsureSize(more)
}

// Un-map the file buffer and close the file handle.
func (file *DataFile) Close() {
	if err := file.Buf.Unmap(); err != nil {
		tdlog.CritNoRepeat("Failed to unmap %s: %v", file.Path, err)
	}
	if err := file.Fh.Close(); err != nil {
		tdlog.CritNoRepeat("Failed to close %s: %v", file.Path, err)
	}
}

// Clear the entire file and resize it to initial size.
func (file *DataFile) Clear() (err error) {
	file.Close()
	if err = os.Remove(file.Path); err != nil {
		return
	} else if file.Fh, err = os.OpenFile(file.Path, os.O_CREATE|os.O_RDWR, 0600); err != nil {
		return
	} else if err = file.overwriteWithZero(0, file.Growth); err != nil {
		return
	} else if file.Buf, err = gommap.Map(file.Fh); err != nil {
		return
	}
	file.Used, file.Size = 0, file.Growth
	tdlog.Infof("%s cleared: %d of %d bytes in-use", file.Path, file.Used, file.Size)
	return
}
