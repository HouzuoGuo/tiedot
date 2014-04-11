// Common data file features.
package data

import (
	"github.com/HouzuoGuo/tiedot/gommap"
	"github.com/HouzuoGuo/tiedot/tdlog"
	"os"
)

// Data file keeps track of the amount of total and used space.
type DataFile struct {
	Path               string
	Size, Used, Growth int
	Fh                 *os.File
	Buf                gommap.MMap
}

// Open a data file that grows by the specified size.
func OpenDataFile(path string, growth int) (file *DataFile, err error) {
	file = &DataFile{Path: path, Growth: growth}
	if file.Fh, err = os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600); err != nil {
		return
	}
	var size int64
	if size, err = file.Fh.Seek(0, os.SEEK_END); err != nil {
		return
	}
	// Ensure the file is not smaller than file growth
	if file.Size = int(size); file.Size < file.Growth {
		if err = file.EnsureSize(growth); err != nil {
			return
		}
	}
	if file.Buf, err = gommap.Map(file.Fh, gommap.RDWR, 0); err != nil {
		return
	}
	for i := file.Size - 1; i >= 0; i-- {
		if file.Buf[i] != 0 {
			file.Used = i + 1
			break
		}
	}
	tdlog.Printf("%s opened: %d of %d bytes in-use", path, file.Used, file.Size)
	return
}

// Ensure there is enough room for that many bytes of data.
func (file *DataFile) EnsureSize(more int) (err error) {
	if file.Used+more <= file.Size {
		return
	}
	if file.Buf != nil {
		if err = file.Buf.Unmap(); err != nil {
			return
		}
	}
	if err = os.Truncate(file.Path, int64(file.Size+file.Growth)); err != nil {
		return
	} else if file.Buf, err = gommap.Map(file.Fh, gommap.RDWR, 0); err != nil {
		return
	}
	file.Size += file.Growth
	tdlog.Printf("%s grown: %d -> %d bytes (%d bytes in-use)", file.Path, file.Size-file.Growth, file.Size, file.Used)
	return file.EnsureSize(more)
}

// Synchronize file buffer onto underlying storage device.
func (file *DataFile) Sync() (err error) {
	return file.Buf.Flush()
}

// Unmap the file buffer and close the file handle.
func (file *DataFile) Close() (err error) {
	if err = file.Buf.Unmap(); err != nil {
		return
	}
	return file.Fh.Close()
}

// Clear the entire file and resize it to initial size.
func (file *DataFile) Clear() (err error) {
	if err = file.Buf.Unmap(); err != nil {
		return
	} else if err = os.Truncate(file.Path, 0); err != nil {
		return
	} else if err = os.Truncate(file.Path, int64(file.Growth)); err != nil {
		return
	} else if file.Buf, err = gommap.Map(file.Fh, gommap.RDWR, 0); err != nil {
		return
	}
	file.Used, file.Size = 0, file.Growth
	tdlog.Printf("%s cleared: %d of %d bytes in-use", file.Path, file.Used, file.Size)
	return
}
