// Common data file features - enlarge, close, close, etc.

package data

import (
	"os"

	"github.com/HouzuoGuo/tiedot/gommap"
	"github.com/HouzuoGuo/tiedot/tdlog"
	"log"
)

// Data file keeps track of the amount of total and used space.
type DataFile struct {
	Path               string
	Size, Used, Growth int
	Fh                 *os.File
	Buf                gommap.MMap
}

// Return true if the buffer begins with 64 consecutive zero bytes.
func LooksEmpty(buf gommap.MMap) bool {
	upTo := 1024
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

// Open a data file that grows by the specified size.
func OpenDataFile(path string, growth int) (file *DataFile, err error) {
	file = &DataFile{Path: path, Growth: growth}
	if file.Fh, err = os.OpenFile(file.Path, os.O_CREATE|os.O_RDWR, 0600); err != nil {
		return
	}
	var size int64
	if size, err = file.Fh.Seek(0, os.SEEK_END); err != nil {
		return
	}
	// Ensure the file is not smaller than file growth
	if file.Size = int(size); file.Size < file.Growth {
		if err = file.EnsureSize(file.Growth); err != nil {
			return
		}
	}
	if file.Buf == nil {
		file.Buf, err = gommap.Map(file.Fh)
	}
	defer tdlog.Infof("%s opened: %d of %d bytes in-use", file.Path, file.Used, file.Size)
	// Bi-sect file buffer to find out how much space is in-use
	log.Printf("Trying to determine %s (%d) 's usage", path, file.Size)
	for low, mid, high := 0, file.Size/2, file.Size; ; {
		log.Printf("before low\t%d\tmid\t%d\thigh\t%d\tlen\t%d", low, mid, high, len(file.Buf))
		switch {
		case high-mid == 1:
			if LooksEmpty(file.Buf[mid:]) {
				if mid > 0 && LooksEmpty(file.Buf[mid-1:]) {
					file.Used = mid - 1
				} else {
					file.Used = mid
				}
				return
			}
			file.Used = high
			return
		case LooksEmpty(file.Buf[mid:]):
			high = mid
			mid = low + (mid-low)/2
		default:
			low = mid
			mid = mid + (high-mid)/2
		}
		log.Printf("after  low\t%d\tmid\t%d\thigh\t%d\tlen\t%d", low, mid, high, len(file.Buf))
	}
	return
}

// Fill up portion of a file with 0s.
func (file *DataFile) overwriteWithZero(from int, size int) (err error) {
	if _, err = file.Fh.Seek(int64(from), os.SEEK_SET); err != nil {
		return
	}
	zeroSize := 1048576 * 8 // Fill 8 MB at a time
	zero := make([]byte, zeroSize)
	for i := 0; i < size; i += zeroSize {
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

// Ensure there is enough room for that many bytes of data.
func (file *DataFile) EnsureSize(more int) (err error) {
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
func (file *DataFile) Close() (err error) {
	if err = file.Buf.Unmap(); err != nil {
		return
	}
	return file.Fh.Close()
}

// Clear the entire file and resize it to initial size.
func (file *DataFile) Clear() (err error) {
	if err = file.Close(); err != nil {
		return
	} else if err = os.Truncate(file.Path, 0); err != nil {
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
