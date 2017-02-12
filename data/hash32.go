// +build 386 arm

package data

const (
	HT_FILE_GROWTH  = 8 * 1048576 // Hash table file initial size & file growth
	HASH_BITS       = 14          // Number of hash key bits
	INITIAL_BUCKETS = 16384       // Initial number of buckets == 2 ^ HASH_BITS
)

// Return the portion (first HASH_BITS bytes) used for allocating the entry. The integer smearing process does not apply to 32-bit system.
func HashKey(key int) int {
	return key & ((1 << HASH_BITS) - 1)
}
