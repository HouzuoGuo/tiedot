/* A static hash table made of uint64 key-value pairs. */
package chunkfile

import (
	"encoding/binary"
	"github.com/HouzuoGuo/tiedot/commonfile"
	"github.com/HouzuoGuo/tiedot/tdlog"
	"math"
)

const (
	HT_FILE_SIZE       = uint64(1024 * 1024 * 8) // Size of a hash table, it may grow by this size
	ENTRY_VALID        = byte(1)                 // Entry valid flag
	ENTRY_INVALID      = byte(0)                 // Entry invalid flag
	ENTRY_SIZE         = uint64(1 + 10 + 10)     // Size of entry header - validity (byte), hash key (uint64) and value (uint64)
	BUCKET_HEADER_SIZE = uint64(10)              // Size of bucket header - next bucket in chain (uint64)
	// Hash table configuration
	PER_BUCKET  = uint64(15)
	BUCKET_SIZE = uint64(PER_BUCKET*ENTRY_SIZE + BUCKET_HEADER_SIZE)
	HASH_BITS   = uint64(14)
	// INITIAL_BUCKETS = 2 to the power of HASH_BITS
	INITIAL_BUCKETS = uint64(16384)
)

type HashTable struct {
	Path       []string
	File       commonfile.File
	NumBuckets uint64 // Total number of buckets
}

// Open a hash table file.
func OpenHash(name string, path []string) (ht HashTable, err error) {
	file, err := commonfile.Open(name, HT_FILE_SIZE)
	if err != nil {
		return
	}
	ht = HashTable{File: file, Path: path}
	ht.calculateSizeInfo()
	return ht, nil
}

// Calculate used size, total size, total number of buckets.
func (ht *HashTable) calculateSizeInfo() {
	// Find out how many buckets there are in table - hence the amount of used space
	// .. assume the entire file is Full of buckets
	ht.File.UsedSize = ht.File.Size
	ht.NumBuckets = ht.File.Size / BUCKET_SIZE
	// .. starting from every head bucket, find the longest chain
	longestBucketChain := INITIAL_BUCKETS
	for i := uint64(0); i < INITIAL_BUCKETS; i++ {
		lastBucket := ht.lastBucket(i)
		if lastBucket+1 > longestBucketChain && lastBucket+1 <= ht.NumBuckets {
			longestBucketChain = lastBucket + 1
		}
	}
	// .. the longest chain tells amount of used space
	ht.NumBuckets = longestBucketChain
	usedSize := ht.NumBuckets * BUCKET_SIZE
	// Grow the file, if it is not yet large enough for all the buckets used
	if usedSize > ht.File.Size {
		ht.File.UsedSize = ht.File.Size
		ht.File.CheckSizeAndEnsure(((usedSize-ht.File.Size)/BUCKET_SIZE + 1) * BUCKET_SIZE)
	}
	ht.File.UsedSize = usedSize
	tdlog.Printf("%s has %d buckets, and %d bytes out of %d bytes in-use", ht.File.Name, ht.NumBuckets, ht.File.UsedSize, ht.File.Size)
}

// Return the number (not address) of next chained bucket, 0 if there is not any.
func (ht *HashTable) NextBucket(bucket uint64) uint64 {
	if bucket >= ht.NumBuckets {
		return 0
	}
	bucketAddr := bucket * BUCKET_SIZE
	if next, _ := binary.Uvarint(ht.File.Buf[bucketAddr : bucketAddr+BUCKET_HEADER_SIZE]); next == 0 {
		return 0
	} else if next <= bucket {
		tdlog.Errorf("ERROR: Bucket loop in hash table %s at bucket no.%d, address %d", ht.File.Name, bucket, bucketAddr)
		return 0
	} else if next >= ht.NumBuckets || next < INITIAL_BUCKETS {
		tdlog.Errorf("ERROR: Bad bucket refernece (%d is out of range %d - %d) in %s", next, INITIAL_BUCKETS, ht.NumBuckets, ht.File.Name)
		return 0
	} else {
		return next
	}
}

// Return the last bucket number (not address) in chain.
func (ht *HashTable) lastBucket(bucket uint64) uint64 {
	curr := bucket
	for {
		next := ht.NextBucket(curr)
		if next == 0 {
			return curr
		}
		curr = next
	}
}

// Grow a new bucket on the chain of buckets.
func (ht *HashTable) grow(bucket uint64) {
	ht.File.CheckSizeAndEnsure(BUCKET_SIZE)
	// Write down new bucket number
	lastBucketAddr := ht.lastBucket(bucket) * BUCKET_SIZE
	binary.PutUvarint(ht.File.Buf[lastBucketAddr:lastBucketAddr+10], ht.NumBuckets)
	ht.File.UsedSize += BUCKET_SIZE
	ht.NumBuckets += 1
}

// Return a hash key to be used by hash table by masking non-key bits.
func (ht *HashTable) HashKey(key uint64) uint64 {
	return key & ((1 << HASH_BITS) - 1)
}

// Clear all index entries, return to the initial size as well.
func (ht *HashTable) Clear() {
	ht.File.Clear()
	// Recalculate size information
	ht.calculateSizeInfo()
}

// Put a new key-value pair.
func (ht *HashTable) Put(key, val uint64) {
	var bucket, entry uint64 = ht.HashKey(key), 0
	for {
		entryAddr := bucket*BUCKET_SIZE + BUCKET_HEADER_SIZE + entry*ENTRY_SIZE
		if ht.File.Buf[entryAddr] != ENTRY_VALID {
			ht.File.Buf[entryAddr] = ENTRY_VALID
			binary.PutUvarint(ht.File.Buf[entryAddr+1:entryAddr+11], key)
			binary.PutUvarint(ht.File.Buf[entryAddr+11:entryAddr+21], val)
			return
		}
		if entry++; entry == PER_BUCKET {
			entry = 0
			if bucket = ht.NextBucket(bucket); bucket == 0 {
				ht.grow(ht.HashKey(key))
				ht.Put(key, val)
				return
			}
		}
	}
}

// Get key-value pairs.
func (ht *HashTable) Get(key, limit uint64, filter func(uint64, uint64) bool) (keys, vals []uint64) {
	// This function is partially inlined in chunkcol.go
	var count, entry, bucket uint64 = 0, 0, ht.HashKey(key)
	if limit == 0 {
		keys = make([]uint64, 0, 10)
		vals = make([]uint64, 0, 10)
	} else {
		keys = make([]uint64, 0, limit)
		vals = make([]uint64, 0, limit)
	}
	for {
		entryAddr := bucket*BUCKET_SIZE + BUCKET_HEADER_SIZE + entry*ENTRY_SIZE
		entryKey, _ := binary.Uvarint(ht.File.Buf[entryAddr+1 : entryAddr+11])
		entryVal, _ := binary.Uvarint(ht.File.Buf[entryAddr+11 : entryAddr+21])
		if ht.File.Buf[entryAddr] == ENTRY_VALID {
			if entryKey == key && filter(entryKey, entryVal) {
				keys = append(keys, entryKey)
				vals = append(vals, entryVal)
				if count++; count == limit {
					return
				}
			}
		} else if entryKey == 0 && entryVal == 0 {
			return
		}
		if entry++; entry == PER_BUCKET {
			entry = 0
			if bucket = ht.NextBucket(bucket); bucket == 0 {
				return
			}
		}
	}
}

// Remove specific key-value pair.
func (ht *HashTable) Remove(key, val uint64) {
	var entry, bucket uint64 = 0, ht.HashKey(key)
	for {
		entryAddr := bucket*BUCKET_SIZE + BUCKET_HEADER_SIZE + entry*ENTRY_SIZE
		entryKey, _ := binary.Uvarint(ht.File.Buf[entryAddr+1 : entryAddr+11])
		entryVal, _ := binary.Uvarint(ht.File.Buf[entryAddr+11 : entryAddr+21])
		if ht.File.Buf[entryAddr] == ENTRY_VALID {
			if entryKey == key && entryVal == val {
				ht.File.Buf[entryAddr] = ENTRY_INVALID
				return
			}
		} else if entryKey == 0 && entryVal == 0 {
			return
		}
		if entry++; entry == PER_BUCKET {
			entry = 0
			if bucket = ht.NextBucket(bucket); bucket == 0 {
				return
			}
		}
	}
}

// Return all entries in the hash table.
func (ht *HashTable) GetAll(limit uint64) (keys, vals []uint64) {
	prealloc := limit
	if prealloc == 0 {
		prealloc = INITIAL_BUCKETS * PER_BUCKET / 2
	}
	keys = make([]uint64, 0, prealloc)
	vals = make([]uint64, 0, prealloc)
	counter := uint64(0)
	for head := uint64(0); head < uint64(math.Pow(2, float64(HASH_BITS))); head++ {
		var entry, bucket uint64 = 0, head
		for {
			entryAddr := bucket*BUCKET_SIZE + BUCKET_HEADER_SIZE + entry*ENTRY_SIZE
			entryKey, _ := binary.Uvarint(ht.File.Buf[entryAddr+1 : entryAddr+11])
			entryVal, _ := binary.Uvarint(ht.File.Buf[entryAddr+11 : entryAddr+21])
			if ht.File.Buf[entryAddr] == ENTRY_VALID {
				counter++
				keys = append(keys, entryKey)
				vals = append(vals, entryVal)
				if counter == limit {
					return
				}
			} else if entryKey == 0 && entryVal == 0 {
				break
			}
			if entry++; entry == PER_BUCKET {
				entry = 0
				if bucket = ht.NextBucket(bucket); bucket == 0 {
					return
				}
			}
		}
	}
	return
}
