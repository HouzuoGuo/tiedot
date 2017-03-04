// +build !386,!arm

package data

const (
	HT_FILE_GROWTH  = 32 * 1048576 // Hash table file initial size & file growth
	HASH_BITS       = 16           // Number of hash key bits
	INITIAL_BUCKETS = 65536        // Initial number of buckets == 2 ^ HASH_BITS
)

// Smear the integer entry key and return the portion (first HASH_BITS bytes) used for allocating the entry.
func HashKey(key int) int {
	// ========== Integer-smear start =======
	key = key ^ (key >> 4)
	key = (key ^ 0xdeadbeef) + (key << 5)
	key = key ^ (key >> 11)
	// ========== Integer-smear end =========
	return key & ((1 << HASH_BITS) - 1)
}
