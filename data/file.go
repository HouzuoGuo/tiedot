package data

import (
	"github.com/HouzuoGuo/tiedot/gommap"
	"os"
)

type DataFile struct {
	Path               string
	Size, Used, Growth int64
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

func OpenDataFile(path string, growth int64) (file *DataFile, err error) {
	file = &DataFile{Path: path, Growth: growth}
	if file.Fh, err = os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600); err != nil {
		return
	} else if file.Size, err = file.Fh.Seek(0, os.SEEK_END); err != nil {
		return
	} else if file.Size < file.Growth {
		if err = file.EnsureSize(growth); err != nil {
			return
		}
	} else if file.Buf, err = gommap.Map(file.Fh, gommap.RDWR, 0); err != nil {
		return
	}
	file.CalculateUsedSize()
	return
}

func (file *DataFile) EnsureSize(more int64) (err error) {
	if file.Used+more <= file.Size {
		return
	} else if file.Buf != nil {
		if err = file.Buf.Unmap(); err != nil {
			return
		}
	} else if err = os.Truncate(file.Path, file.Size+file.Growth); err != nil {
		return
	} else if file.Buf, err = gommap.Map(file.Fh, gommap.RDWR, 0); err != nil {
		return
	}
	file.Size += file.Growth
	return file.EnsureSize(more)
}

func (file *DataFile) CalculateUsedSize() {
	for low, mid, high := int64(0), file.Size/2, file.Size; ; {
		if high-mid == 1 {
			if Twenty0s(file.Buf[mid:]) {
				file.Used = mid
			} else {
				file.Used = high
			}
			return
		} else if Twenty0s(file.Buf[mid:]) {
			high, mid = mid, (low+mid)/2
		} else {
			low, mid = mid, (high+mid)/2
		}
	}
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
	} else if err = os.Truncate(file.Path, file.Growth); err != nil {
		return
	} else if file.Buf, err = gommap.Map(file.Fh, gommap.RDWR, 0); err != nil {
		return
	}
	file.Used, file.Size = 0, file.Growth
	return
}
