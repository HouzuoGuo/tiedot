/* Common data file features. */
package file

import (
	"github.com/HouzuoGuo/tiedot/gommap"
	"log"
	"os"
)

const (
	FILE_GROWTH_INCREMENTAL = uint64(16777216)
	DEFAULT_GROWTH          = uint64(33554432)
)

type File struct {
	Name                   string   // File path and name
	Fh                     *os.File // File handle (in operating system)
	UsedSize, Size, Growth uint64
	Buf                    gommap.MMap // Mapped file buffer
}

// Open the file, or create it if non-existing.
func Open(name string) (file *File, err error) {
	file = &File{Name: name, Growth: DEFAULT_GROWTH}
	// Open file (get a handle) and determine its size
	if file.Fh, err = os.OpenFile(name, os.O_CREATE|os.O_RDWR, 0600); err != nil {
		return
	}
	fsize, err := file.Fh.Seek(0, os.SEEK_END)
	if err != nil {
		return
	}
	file.Size = uint64(fsize)
	file.recalculateGrowth()
	if file.Size == 0 {
		// Grow the file if it appears too small
		file.CheckSizeAndEnsure(file.Growth)
		return
	}
	// Map the file into memory buffer
	if file.Buf, err = gommap.Map(file.Fh, gommap.RDWR, 0); err != nil {
		return
	}
	// Bi-sect file buffer to find out how much space in the file is actively in-use
	for low, mid, high := uint64(0), file.Size/2, file.Size; ; {
		switch {
		case high-mid == 1:
			if file.Buf[mid] == 0 {
				if file.Buf[mid-1] == 0 {
					file.UsedSize = mid - 1
				} else {
					file.UsedSize = mid
				}
				return
			}
			file.UsedSize = high
			return
		case file.Buf[mid] == 0:
			high = mid
			mid = low + (mid-low)/2
		default:
			low = mid
			mid = mid + (high-mid)/2
		}
	}
	log.Printf("%s has %d bytes out of %d bytes in-use", name, file.UsedSize, file.Size)
	return
}

func (file *File) recalculateGrowth() {
	switch {
	case file.Size < 134217728: // 128MB
		file.Growth = 33554432
	default: // above 128MB
		file.Growth = 134217728
	}
}

// Return true only if the file has enough room for more data.
func (file *File) CheckSize(more uint64) bool {
	return file.UsedSize+more <= file.Size
}

// Ensure that the file has enough room for more data. Grow the file if necessary.
func (file *File) CheckSizeAndEnsure(more uint64) {
	if file.UsedSize+more <= file.Size {
		return
	}
	file.recalculateGrowth()
	// Unmap file buffer
	var err error
	if file.Buf != nil {
		if err = file.Buf.Unmap(); err != nil {
			panic(err)
		}
	}
	if _, err = file.Fh.Seek(0, os.SEEK_END); err != nil {
		panic(err)
	}
	// Grow file size (incrementally)
	zeroBuf := make([]byte, FILE_GROWTH_INCREMENTAL)
	for i := uint64(0); i < file.Growth; i += FILE_GROWTH_INCREMENTAL {
		var slice []byte
		if i+FILE_GROWTH_INCREMENTAL > file.Growth {
			slice = zeroBuf[0 : i+FILE_GROWTH_INCREMENTAL-file.Growth]
		} else {
			slice = zeroBuf
		}
		if _, err = file.Fh.Write(slice); err != nil {
			panic(err)
		}
	}
	if err = file.Fh.Sync(); err != nil {
		panic(err)
	}
	// Re-map the (now larger) file buffer
	if file.Buf, err = gommap.Map(file.Fh, gommap.RDWR, 0); err != nil {
		panic(err)
	}
	file.Size += file.Growth
	log.Printf("File %s has grown %d bytes\n", file.Name, file.Growth)
	file.CheckSizeAndEnsure(more)
}

// Synchronize file buffer with underlying storage device.
func (file *File) Flush() error {
	return file.Buf.Flush()
}

// Close the file.
func (file *File) Close() (err error) {
	if err = file.Buf.Unmap(); err != nil {
		return
	}
	return file.Fh.Close()
}
