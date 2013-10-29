/* Common data file features. */
package file

import (
	"errors"
	"fmt"
	"log"
	"loveoneanother.at/tiedot/gommap"
	"os"
)

const FILE_GROWTH_INCREMENTAL = uint64(16777216)

type File struct {
	Name                 string
	Fh                   *os.File
	Append, Size, Growth uint64
	Buf                  gommap.MMap
}

// Open (create if non-exist) the file.
func Open(name string, growth uint64) (file *File, err error) {
	if growth < 1 {
		err = errors.New(fmt.Sprintf("Opening %s, file growth (%d) is too small", name, growth))
	}
	file = &File{Name: name, Growth: growth}
	if file.Fh, err = os.OpenFile(name, os.O_CREATE|os.O_RDWR, 0600); err != nil {
		return
	}
	fsize, err := file.Fh.Seek(0, os.SEEK_END)
	if err != nil {
		return
	}
	file.Size = uint64(fsize)
	if file.Size == 0 {
		file.CheckSizeAndEnsure(file.Growth)
		return
	}

	if file.Buf, err = gommap.Map(file.Fh, gommap.RDWR, 0); err != nil {
		return
	}
	// find append position
	for pos := file.Size - 1; pos > 0; pos-- {
		if file.Buf[pos] != 0 {
			file.Append = pos + 1
			break
		}
	}
	return
}

// Ensure the file has room for more data.
func (file *File) CheckSize(more uint64) bool {
	return file.Append+more <= file.Size
}

// Ensure the file ahs room for more data.
func (file *File) CheckSizeAndEnsure(more uint64) {
	if file.Append+more <= file.Size {
		return
	}
	var err error
	if file.Buf != nil {
		if err = file.Buf.Unmap(); err != nil {
			panic(err)
		}
	}
	if _, err = file.Fh.Seek(0, os.SEEK_END); err != nil {
		panic(err)
	}
	// grow the file incrementally
	zeroBuf := make([]byte, FILE_GROWTH_INCREMENTAL)
	for i := uint64(0); i < file.Growth; i += FILE_GROWTH_INCREMENTAL {
		var slice []byte
		if i > file.Growth {
			slice = zeroBuf[0:file.Growth]
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
	if file.Buf, err = gommap.Map(file.Fh, gommap.RDWR, 0); err != nil {
		panic(err)
	}
	file.Size += file.Growth
	log.Printf("File %s has grown %d bytes\n", file.Name, file.Growth)
	file.CheckSizeAndEnsure(more)
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
