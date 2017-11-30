package data

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
)

// Collection config holds configuration data for Collections
type Data struct {
	DocMaxRoom     int
	DocHeader      int
	Padding        string
	ColFileGrowth  int
	LenPadding     int `json:"-"`
	EntrySize      int
	BucketHeader   int
	PerBucket      int
	BucketSize     int `json:"-"`
	HTFileGrowth   int
	HashBits       uint
	InitialBuckets int
}

func New(path string) (data *Data, err error) {
	var file *os.File
	var j []byte

	if err = os.MkdirAll(path, 0700); err != nil {
		return
	}

	filePath := fmt.Sprintf("%s/data-config.json", path)

	// set the default dataConfig
	data = defaultData()

	// try to open the file
	if file, err = os.OpenFile(filePath, os.O_RDONLY, 0644); err != nil {
		if _, ok := err.(*os.PathError); ok {
			// if we could not find the file because it doesn't exist, lets create it
			// so the database always runs with these settings
			err = nil

			if file, err = os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY, 0644); err != nil {
				return
			}

			j, err = json.MarshalIndent(data, "", "  ")
			if err != nil {
				return
			}

			if _, err = file.Write(j); err != nil {
				return
			}

		} else {
			return
		}
	} else {
		// if we find the file we will leave it as it is and merge
		// it into the default
		var b []byte
		if b, err = ioutil.ReadAll(file); err != nil {
			return
		}

		if err = json.Unmarshal(b, data); err != nil {
			return
		}
	}

	data.LenPadding = len(data.Padding)
	data.BucketSize = data.BucketHeader + data.PerBucket*data.EntrySize

	return
}

func defaultData() *Data {
	data := &Data{
		DocMaxRoom:     2 * 1048576,
		DocHeader:      1 + 10,
		Padding:        strings.Repeat(" ", 128),
		ColFileGrowth:  COL_FILE_GROWTH,
		EntrySize:      1 + 10 + 10,
		BucketHeader:   10,
		PerBucket:      16,
		HTFileGrowth:   HT_FILE_GROWTH,
		HashBits:       HASH_BITS,
		InitialBuckets: INITIAL_BUCKETS,
	}

	data.LenPadding = len(data.Padding)
	data.BucketSize = data.BucketHeader + data.PerBucket*data.EntrySize

	return data
}
