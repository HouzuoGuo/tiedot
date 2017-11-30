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
	d := &Data{
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

	if err := assertCorrectData(tmp, d); err != nil {
		t.Fatal(err)
	}
}

/*
  Set up the configs when there is already a file
*/
func TestConfiguredJsonConfig(t *testing.T) {
	d := &Data{
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

	if err := assertCorrectData(tmp, d); err != nil {
		t.Fatal(err)
	}
}

func assertCorrectData(path string, assertData *Data) (err error) {
	var d *Data
	// Start by making sure an empty initializion works
	if d, err = New(path); err != nil {
		return
	}

	if err = assertDataEqual(d, assertData); err != nil {
		return
	}

	return
}

func assertDataEqual(d1, d2 *Data) error {
	if d1.DocMaxRoom != d2.DocMaxRoom {
		return fmt.Errorf("DocMaxRoom configs differ %s != %s", d1.DocMaxRoom, d2.DocMaxRoom)
	}

	if d1.DocHeader != d2.DocHeader {
		return fmt.Errorf("DocHeader configs differ %s != %s", d1.DocHeader, d2.DocHeader)
	}

	if d1.Padding != d2.Padding {
		return fmt.Errorf("Padding configs differ %s != %s", d1.Padding, d2.Padding)
	}

	if d1.ColFileGrowth != d2.ColFileGrowth {
		return fmt.Errorf("ColFileGrowth configs differ %s != %s", d1.ColFileGrowth, d2.ColFileGrowth)
	}

	if d1.EntrySize != d2.EntrySize {
		return fmt.Errorf("EntrySize configs differ %s != %s", d1.EntrySize, d2.EntrySize)
	}

	if d1.BucketHeader != d2.BucketHeader {
		return fmt.Errorf("BucketHeader configs differ %s != %s", d1.BucketHeader, d2.BucketHeader)
	}

	if d1.PerBucket != d2.PerBucket {
		return fmt.Errorf("PerBucket configs differ %s != %s", d1.PerBucket, d2.PerBucket)
	}

	if d1.HashBits != d2.HashBits {
		return fmt.Errorf("HashBits configs differ %s != %s", d1.HashBits, d2.HashBits)
	}

	if d1.HTFileGrowth != d2.HTFileGrowth {
		return fmt.Errorf("HTFileGrowth configs differ %s != %s", d1.HTFileGrowth, d2.HTFileGrowth)
	}

	if d1.InitialBuckets != d2.InitialBuckets {
		return fmt.Errorf("InitialBuckets configs differ %s != %s", d1.InitialBuckets, d2.InitialBuckets)
	}

	return nil
}
