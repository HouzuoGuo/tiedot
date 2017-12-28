package data

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
)

const (
	DefaultDocMaxRoom = 2 * 1048576 // DefaultDocMaxRoom is the default maximum size a single document may never exceed.
	DocHeader         = 1 + 10      // DocHeader is the size of document header fields.
	EntrySize         = 1 + 10 + 10 // EntrySize is the size of a single hash table entry.
	BucketHeader      = 10          // BucketHeader is the size of hash table bucket's header fields.
)

/*
Config consists of tuning parameters initialised once upon creation of a new database, the properties heavily influence
performance characteristics of all collections in a database. Adjust with care!
*/
type Config struct {
	DocMaxRoom    int  // DocMaxRoom is the maximum size of a single document that will ever be accepted into database.
	ColFileGrowth int  // ColFileGrowth is the size (in bytes) to grow collection data file when new documents have to fit in.
	PerBucket     int  // PerBucket is the number of entries pre-allocated to each hash table bucket.
	HTFileGrowth  int  /// HTFileGrowth is the size (in bytes) to grow hash table file to fit in more entries.
	HashBits      uint // HashBits is the number of bits to consider for hashing indexed key, also determines the initial number of buckets in a hash table file.

	InitialBuckets int    `json:"-"` // InitialBuckets is the number of buckets initially allocated in a hash table file.
	Padding        string `json:"-"` // Padding is pre-allocated filler (space characters) for new documents.
	LenPadding     int    `json:"-"` // LenPadding is the calculated length of Padding string.
	BucketSize     int    `json:"-"` // BucketSize is the calculated size of each hash table bucket.
}

// CalculateConfigConstants assignes internal field values to calculation results derived from other fields.
func (conf *Config) CalculateConfigConstants() {
	conf.Padding = strings.Repeat(" ", 128)
	conf.LenPadding = len(conf.Padding)

	conf.BucketSize = BucketHeader + conf.PerBucket*EntrySize
	conf.InitialBuckets = 1 << conf.HashBits
}

// CreateOrReadConfig creates default performance configuration underneath the input database directory.
func CreateOrReadConfig(path string) (conf *Config, err error) {
	var file *os.File
	var j []byte

	if err = os.MkdirAll(path, 0700); err != nil {
		return
	}

	filePath := fmt.Sprintf("%s/data-config.json", path)

	// set the default dataConfig
	conf = defaultConfig()

	// try to open the file
	if file, err = os.OpenFile(filePath, os.O_RDONLY, 0644); err != nil {
		if _, ok := err.(*os.PathError); ok {
			// if we could not find the file because it doesn't exist, lets create it
			// so the database always runs with these settings
			err = nil

			if file, err = os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY, 0644); err != nil {
				return
			}

			j, err = json.MarshalIndent(conf, "", "  ")
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

		if err = json.Unmarshal(b, conf); err != nil {
			return
		}
	}

	conf.CalculateConfigConstants()
	return
}

func defaultConfig() *Config {
	/*
		The default configuration matches the constants defined in tiedot version 3.2 and older. They correspond to ~16MB
		of space per computer CPU core being pre-allocated to each collection.
	*/
	ret := &Config{
		DocMaxRoom:    DefaultDocMaxRoom,
		ColFileGrowth: COL_FILE_GROWTH,
		PerBucket:     16,
		HTFileGrowth:  HT_FILE_GROWTH,
		HashBits:      HASH_BITS,
	}

	ret.CalculateConfigConstants()

	return ret
}
