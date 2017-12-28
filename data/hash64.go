// +build !386,!arm

package data

const (
	HT_FILE_GROWTH = 32 * 1048576 // Default hash table file initial size & file growth
	HASH_BITS      = 16           // Default number of hash key bits
)

// Smear the integer entry key and return the portion (first HASH_BITS bytes) used for allocating the entry.
func (conf *Config) HashKey(key int) int {
	// ========== Integer-smear start =======
	key = key ^ (key >> 4)
	key = (key ^ 0xdeadbeef) + (key << 5)
	key = key ^ (key >> 11)
	// ========== Integer-smear end =========
	return key & ((1 << conf.HashBits) - 1)
}
