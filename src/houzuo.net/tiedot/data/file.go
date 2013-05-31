package data

import (
	"errors"
	"fmt"
	"os"
	"syscall"
)

type File struct {
	Name   string
	Size   uint64
	Growth uint64
	Append uint64
	Buf    []byte
	Fh     *os.File
}

// Open or create a File
func Open(name string, growth uint64) (file *File, err error) {
	if growth < 1 {
		err = errors.New(fmt.Sprintf("File growth must be > 0 (%d given)", growth))
		return
	}
	file = &File{Name: name, Growth: growth}
	if file.Fh, err = os.OpenFile(name, os.O_RDWR|os.O_CREATE, 0600); err != nil {
		return
	}
	fsize, err := file.Fh.Seek(0, os.SEEK_END)
	if err != nil {
		return
	}
	file.Size = uint64(fsize)
	if int(fsize) < 0 {
		panic(fmt.Sprintf("File %s is too large to open", file.Name))
	}
	if file.Buf, err = syscall.Mmap(int(file.Fh.Fd()), 0, int(file.Size), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED); err != nil {
		return
	}
	// find append position (closest byte 0 to position 0)
	if file.Size == 0 {
		err = file.Ensure(file.Growth)
		return
	}
	for low, high, mid := uint64(0), file.Size, file.Size/2; ; {
		if high-mid == 1 {
			if file.Buf[mid] == 0 {
				file.Append = mid
				return
			}
			if high == file.Size {
				err = file.Ensure(file.Growth)
				return
			}
			file.Append = high
			return
		}
		if file.Buf[mid] == 0 {
			high = mid
			mid = low + (mid-low)/2
		} else {
			low = mid
			mid = mid + (high-mid)/2
		}
	}
	return
}

// Ensure the File has room for more data
func (file *File) Ensure(more uint64) (err error) {
	if file.Append+more < file.Size {
		return
	}

	// there is not enough room
	if file.Buf != nil {
		if err = syscall.Munmap(file.Buf); err != nil {
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
	if newLength := int(file.Size + file.Growth); newLength < 0 {
		panic(fmt.Sprintf("File %s is becoming too large", file.Name))
	} else if file.Buf, err = syscall.Mmap(int(file.Fh.Fd()), 0, newLength, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED); err != nil {
		return
	}
	file.Size += file.Growth
	fmt.Fprintf(os.Stderr, "File %s has grown %d bytes", file.Name, file.Growth)
	if err = file.Ensure(more); err != nil {
		return
	}
	return
}

// Close the file
func (file *File) Close() (err error) {
	if err = syscall.Munmap(file.Buf); err != nil {
		return
	}
	err = file.Fh.Close()
	return
}
