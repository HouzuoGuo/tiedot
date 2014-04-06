package data

import (
	"github.com/HouzuoGuo/tiedot/gommap"
	"os"
)

type DataFile struct {
	Path               string
	Size, Used, Growth int
	Fh                 *os.File
	Buf                gommap.MMap
}

func Twenty0s(buf []byte) bool {
	for i, b := range buf {
		if i >= 20 {
			return true
		} else if b != 0 {
			return false
		}
	}
	return true
}

func OpenDataFile(path string, growth int) (file *DataFile, err error) {
	file = &DataFile{Path: path, Growth: growth}
	if file.Fh, err = os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600); err != nil {
		return
	}
	var size int64
	if size, err = file.Fh.Seek(0, os.SEEK_END); err != nil {
		return
	}
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
	return
}

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
	return file.EnsureSize(more)
}

func (file *DataFile) Sync() (err error) {
	return file.Buf.Flush()
}

func (file *DataFile) Close() (err error) {
	if err = file.Buf.Unmap(); err != nil {
		return
	}
	return file.Fh.Close()
}

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
	return
}
