/* Common data file features. */
package file

import (
	"errors"
	"fmt"
	"log"
	"loveoneanother.at/tiedot/gommap"
	"os"
	"sync"
)

type File struct {
	Name                 string
	Fh                   *os.File
	Append, Size, Growth uint64
	Buf                  gommap.MMap
	Sync                 *sync.RWMutex
}

// Open (create if non-exist) the file.
func Open(name string, growth uint64) (file *File, err error) {
	if growth < 1 {
		err = errors.New(fmt.Sprintf("Opening %s, file growth (%d) is too small", name, growth))
	}
	file = &File{Name: name, Growth: growth, Sync: new(sync.RWMutex)}
	if file.Fh, err = os.OpenFile(name, os.O_CREATE|os.O_RDWR, 0600); err != nil {
		return
	}
	fsize, err := file.Fh.Seek(0, os.SEEK_END)
	if err != nil {
		return
	}
	if int(fsize) < 0 {
		log.Panicf("File %s is too large to mmap", name)
	}
	file.Size = uint64(fsize)
	if file.Size == 0 {
		return file, file.Ensure(file.Growth)
	}

	if file.Buf, err = gommap.Map(file.Fh, gommap.RDWR, 0); err != nil {
		return
	}
	// find append position
	for low, mid, high := uint64(0), file.Size/2, file.Size; ; {
		switch {
		case high-mid == 1:
			if file.Buf[mid] == 0 {
				if file.Buf[mid-1] == 0 {
					file.Append = mid - 1
				} else {
					file.Append = mid
				}
				return
			}
			file.Append = high
			return
		case file.Buf[mid] == 0:
			high = mid
			mid = low + (mid-low)/2
		default:
			low = mid
			mid = mid + (high-mid)/2
		}
	}
	return
}

// Ensure the file ahs room for more data.
func (file *File) Ensure(more uint64) (err error) {
	if file.Append+more <= file.Size {
		return
	}
	if file.Buf != nil {
		if err = file.Buf.Unmap(); err != nil {
			return
		}
	}
	if _, err = file.Fh.Seek(0, os.SEEK_END); err != nil {
		return
	}
	if _, err = file.Fh.Write(make([]byte, file.Growth)); err != nil {
		return
	}
	if err = file.Fh.Sync(); err != nil {
		return
	}
	if newSize := int(file.Size + file.Growth); newSize < 0 {
		log.Panicf("File %s is getting too large", file.Name)
	} else if file.Buf, err = gommap.Map(file.Fh, gommap.RDWR, 0); err != nil {
		return
	}
	file.Size += file.Growth
	log.Printf("File %s has grown %d bytes\n", file.Name, file.Growth)
	return file.Ensure(more)
}

// Synchronize mapped region with underlying storage device.
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
