// +build 386 arm

package data

const (
	HT_FILE_GROWTH = 8 * 1048576 // Default hash table file initial size & file growth
	HASH_BITS      = 14          // Default nNumber of hash key bits
)

// Return the portion (first HASH_BITS bytes) used for allocating the entry. The integer smearing process does not apply to 32-bit system.
func (conf *Config) HashKey(key int) int {
	return key & ((1 << conf.HashBits) - 1)
}
