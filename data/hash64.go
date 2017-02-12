// +build !386, !arm

package data

// Smear the integer entry key and return the portion (first HASH_BITS bytes) used for allocating the entry.
func HashKey(key int) int {
	/*
		tiedot should be compiled/run on x86-64 systems.
		If you decide to compile tiedot on 32-bit systems, the following integer-smear algorithm will cause compilation failure
		due to 32-bit interger overflow; therefore you must modify the algorithm.
		Do not remove the integer-smear process, and remember to run test cases to verify your mods.
	*/
	// ========== Integer-smear start =======
	key = key ^ (key >> 4)
	key = (key ^ 0xdeadbeef) + (key << 5)
	key = key ^ (key >> 11)
	// ========== Integer-smear end =========
	return key & ((1 << HASH_BITS) - 1) // Do not modify this line
}
