package data

import (
	"fmt"
	"os"
	"testing"
)

/*
Set up the configs without a file
*/
func TestEmtpyJsonConfig(t *testing.T) {
	d := &Config{
		DocMaxRoom:    2 * 1048576,
		ColFileGrowth: COL_FILE_GROWTH,
		PerBucket:     16,
		HTFileGrowth:  HT_FILE_GROWTH,
		HashBits:      HASH_BITS,
	}
	d.CalculateConfigConstants()

	tmp := "/tmp/tiedot_config_test_empty"
	os.Remove(tmp)
	defer os.Remove(tmp)

	if err := verifyConfigFromPath(tmp, d); err != nil {
		t.Fatal(err)
	}
}

/*
  Set up the configs when there is already a file
*/
func TestConfiguredJsonConfig(t *testing.T) {
	d := &Config{
		DocMaxRoom:    1048576,
		ColFileGrowth: 4194304,
		PerBucket:     16,
		HTFileGrowth:  1048576,
		HashBits:      11,
	}
	d.CalculateConfigConstants()

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

	if err := verifyConfigFromPath(tmp, d); err != nil {
		t.Fatal(err)
	}
}

func verifyConfigFromPath(path string, assertData *Config) (err error) {
	var d *Config
	// Start by making sure an empty initializion works
	if d, err = CreateOrReadConfig(path); err != nil {
		return
	}

	if err = matchConfig(d, assertData); err != nil {
		return
	}

	return
}

func matchConfig(d1, d2 *Config) error {
	if d1.DocMaxRoom != d2.DocMaxRoom {
		return fmt.Errorf("DocMaxRoom configs differ %v != %v", d1.DocMaxRoom, d2.DocMaxRoom)
	}

	if d1.Padding != d2.Padding {
		return fmt.Errorf("Padding configs differ %v != %v", d1.Padding, d2.Padding)
	}

	if d1.ColFileGrowth != d2.ColFileGrowth {
		return fmt.Errorf("ColFileGrowth configs differ %v != %v", d1.ColFileGrowth, d2.ColFileGrowth)
	}

	if d1.PerBucket != d2.PerBucket {
		return fmt.Errorf("PerBucket configs differ %v != %v", d1.PerBucket, d2.PerBucket)
	}

	if d1.HashBits != d2.HashBits {
		return fmt.Errorf("HashBits configs differ %v != %v", d1.HashBits, d2.HashBits)
	}

	if d1.HTFileGrowth != d2.HTFileGrowth {
		return fmt.Errorf("HTFileGrowth configs differ %v != %v", d1.HTFileGrowth, d2.HTFileGrowth)
	}

	if d1.InitialBuckets != d2.InitialBuckets {
		return fmt.Errorf("InitialBuckets configs differ %v != %v", d1.InitialBuckets, d2.InitialBuckets)
	}

	return nil
}
