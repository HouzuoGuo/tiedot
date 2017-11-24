package data

import (
	"fmt"
	"os"
	"strings"
	"testing"
)

/*
Set up the configs without a file
*/
func TestEmtpyJsonConfig(t *testing.T) {
	config := dataConfig{
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

	tmp := "/tmp/tiedot_config_test_empty"
	os.Remove(tmp)
	defer os.Remove(tmp)

	if err := assertCorrectConfig(tmp, config); err != nil {
		t.Fatal(err)
	}
}

/*
  Set up the configs when there is already a file
*/
func TestConfiguredJsonConfig(t *testing.T) {
	config := dataConfig{
		DocMaxRoom:     1048576,
		DocHeader:      1 + 10,
		Padding:        strings.Repeat(" ", 128),
		ColFileGrowth:  4194304,
		EntrySize:      1 + 10 + 10,
		BucketHeader:   10,
		PerBucket:      16,
		HTFileGrowth:   1048576,
		HashBits:       11,
		InitialBuckets: 2048,
	}

	tmp := "/tmp/tiedot_config_test_configured"
	os.Remove(tmp)
	defer os.Remove(tmp)

	if err := os.MkdirAll(tmp, 0700); err != nil {
		t.Fatal(err)
	}

	f, err := os.OpenFile(fmt.Sprintf("%s/data-config.json", tmp), os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		t.Fatal(err)
	}

	_, err = f.Write([]byte(`{"DocMaxRoom": 1048576,"ColFileGrowth": 4194304,"HTFileGrowth": 1048576,"HashBits": 11,"InitialBuckets": 2048}`))

	if err != nil {
		t.Fatal(err)
	}

	f.Close()

	if err := assertCorrectConfig(tmp, config); err != nil {
		t.Fatal(err)
	}
}

func assertCorrectConfig(path string, assertConfig dataConfig) (err error) {
	// Start by making sure an empty initializion works
	if err = InitDataConfig(path); err != nil {
		fmt.Print("one")
		return
	}

	if err = assertConfigsEqual(dataConf, assertConfig); err != nil {
		return
	}

	return
}

func assertConfigsEqual(c1, c2 dataConfig) error {
	if c1.DocMaxRoom != c2.DocMaxRoom {
		return fmt.Errorf("DocMaxRoom configs differ %s != %s", c1.DocMaxRoom, c2.DocMaxRoom)
	}

	if c1.DocHeader != c2.DocHeader {
		return fmt.Errorf("DocHeader configs differ %s != %s", c1.DocHeader, c2.DocHeader)
	}

	if c1.Padding != c2.Padding {
		return fmt.Errorf("Padding configs differ %s != %s", c1.Padding, c2.Padding)
	}

	if c1.ColFileGrowth != c2.ColFileGrowth {
		return fmt.Errorf("ColFileGrowth configs differ %s != %s", c1.ColFileGrowth, c2.ColFileGrowth)
	}

	if c1.EntrySize != c2.EntrySize {
		return fmt.Errorf("EntrySize configs differ %s != %s", c1.EntrySize, c2.EntrySize)
	}

	if c1.BucketHeader != c2.BucketHeader {
		return fmt.Errorf("BucketHeader configs differ %s != %s", c1.BucketHeader, c2.BucketHeader)
	}

	if c1.PerBucket != c2.PerBucket {
		return fmt.Errorf("PerBucket configs differ %s != %s", c1.PerBucket, c2.PerBucket)
	}

	if c1.HashBits != c2.HashBits {
		return fmt.Errorf("HashBits configs differ %s != %s", c1.HashBits, c2.HashBits)
	}

	if c1.HTFileGrowth != c2.HTFileGrowth {
		return fmt.Errorf("HTFileGrowth configs differ %s != %s", c1.HTFileGrowth, c2.HTFileGrowth)
	}

	if c1.InitialBuckets != c2.InitialBuckets {
		return fmt.Errorf("InitialBuckets configs differ %s != %s", c1.InitialBuckets, c2.InitialBuckets)
	}

	return nil
}
