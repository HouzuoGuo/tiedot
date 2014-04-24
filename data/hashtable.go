// Hash table file.
package data

import (
	"encoding/binary"
	"github.com/HouzuoGuo/tiedot/tdlog"
	"math"
)

const (
	HT_FILE_GROWTH  = 64 * 1048576                          // Initial hash table file size; file growth
	ENTRY_SIZE      = 1 + 10 + 10                           // Hash entry: validity, key, value
	BUCKET_HEADER   = 10                                    // Bucker header: next chained bucket number
	PER_BUCKET      = 30                                    // Entries per bucket
	HASH_BITS       = 16                                    // Number of hash key bits
	BUCKET_SIZE     = BUCKET_HEADER + PER_BUCKET*ENTRY_SIZE // Size of a bucket
	INITIAL_BUCKETS = 65536                                 // Initial number of buckets
)

// Hash table is an ordinary data file; it also tracks total number of buckets.
type HashTable struct {
	*DataFile
	numBuckets int
}

// Calculate the hash key of an entry's key.
func HashKey(key int) int {
	return key & ((1 << HASH_BITS) - 1)
}

// Open a hash table file.
func OpenHashTable(path string) (ht *HashTable, err error) {
	ht = new(HashTable)
	ht.DataFile, err = OpenDataFile(path, HT_FILE_GROWTH)
	ht.calculateNumBuckets()
	return
}

// Follow the longest bucket chain to calculate total number of buckets.
func (ht *HashTable) calculateNumBuckets() {
	ht.numBuckets = ht.Size / BUCKET_SIZE
	largestBucketNum := INITIAL_BUCKETS - 1
	for i := 0; i < INITIAL_BUCKETS; i++ {
		lastBucket := ht.lastBucket(i)
		if lastBucket > largestBucketNum && lastBucket < ht.numBuckets {
			largestBucketNum = lastBucket
		}
	}
	ht.numBuckets = largestBucketNum + 1
	usedSize := ht.numBuckets * BUCKET_SIZE
	if usedSize > ht.Size {
		ht.Used = ht.Size
		ht.EnsureSize(usedSize - ht.Used)
	}
	ht.Used = usedSize
}

// Return number of the next chained bucket.
func (ht *HashTable) nextBucket(bucket int) int {
	if bucket >= ht.numBuckets {
		return 0
	}
	bucketAddr := bucket * BUCKET_SIZE
	nextUint, err := binary.Varint(ht.Buf[bucketAddr : bucketAddr+10])
	next := int(nextUint)
	if next == 0 {
		return 0
	} else if err < 0 || next <= bucket || next >= ht.numBuckets || next < INITIAL_BUCKETS {
		tdlog.Errorf("Bad hash table - repair ASAP %s", ht.Path)
		return 0
	} else {
		return next
	}
}

// Return number of the last bucket in chain.
func (ht *HashTable) lastBucket(bucket int) int {
	for curr := bucket; ; {
		next := ht.nextBucket(curr)
		if next == 0 {
			return curr
		}
		curr = next
	}
}

// Chain a new bucket.
func (ht *HashTable) growBucket(bucket int) {
	ht.EnsureSize(BUCKET_SIZE)
	lastBucketAddr := ht.lastBucket(bucket) * BUCKET_SIZE
	binary.PutVarint(ht.Buf[lastBucketAddr:lastBucketAddr+10], int64(ht.numBuckets))
	ht.Used += BUCKET_SIZE
	ht.numBuckets++
}

// Clear the entire hash table.
func (ht *HashTable) Clear() {
	ht.DataFile.Clear()
	ht.calculateNumBuckets()
}

// Store a key-value pair into a vacant entry.
func (ht *HashTable) Put(key, val int) {
	for bucket, entry := HashKey(key), 0; ; {
		entryAddr := bucket*BUCKET_SIZE + BUCKET_HEADER + entry*ENTRY_SIZE
		if ht.Buf[entryAddr] != 1 {
			ht.Buf[entryAddr] = 1
			binary.PutVarint(ht.Buf[entryAddr+1:entryAddr+11], int64(key))
			binary.PutVarint(ht.Buf[entryAddr+11:entryAddr+21], int64(val))
			return
		}
		if entry++; entry == PER_BUCKET {
			entry = 0
			if bucket = ht.nextBucket(bucket); bucket == 0 {
				ht.growBucket(HashKey(key))
				ht.Put(key, val)
				return
			}
		}
	}
}

// Look up values by key.
func (ht *HashTable) Get(key, limit int) (vals []int) {
	if limit == 0 {
		vals = make([]int, 0, 10)
	} else {
		vals = make([]int, 0, limit)
	}
	for count, entry, bucket := 0, 0, HashKey(key); ; {
		entryAddr := bucket*BUCKET_SIZE + BUCKET_HEADER + entry*ENTRY_SIZE
		entryKey, _ := binary.Varint(ht.Buf[entryAddr+1 : entryAddr+11])
		entryVal, _ := binary.Varint(ht.Buf[entryAddr+11 : entryAddr+21])
		if ht.Buf[entryAddr] == 1 {
			if int(entryKey) == key {
				vals = append(vals, int(entryVal))
				if count++; count == limit {
					return
				}
			}
		} else if entryKey == 0 && entryVal == 0 {
			return
		}
		if entry++; entry == PER_BUCKET {
			entry = 0
			if bucket = ht.nextBucket(bucket); bucket == 0 {
				return
			}
		}
	}
}

// Flag a key-value pair as invalid.
func (ht *HashTable) Remove(key, val int) {
	for entry, bucket := 0, HashKey(key); ; {
		entryAddr := bucket*BUCKET_SIZE + BUCKET_HEADER + entry*ENTRY_SIZE
		entryKey, _ := binary.Varint(ht.Buf[entryAddr+1 : entryAddr+11])
		entryVal, _ := binary.Varint(ht.Buf[entryAddr+11 : entryAddr+21])
		if ht.Buf[entryAddr] == 1 {
			if int(entryKey) == key && int(entryVal) == val {
				ht.Buf[entryAddr] = 0
				return
			}
		} else if entryKey == 0 && entryVal == 0 {
			return
		}
		if entry++; entry == PER_BUCKET {
			entry = 0
			if bucket = ht.nextBucket(bucket); bucket == 0 {
				return
			}
		}
	}
}

// Return all entries in the hash table.
func (ht *HashTable) AllEntries(limit int) (keys, vals []int) {
	prealloc := limit
	if prealloc == 0 {
		prealloc = INITIAL_BUCKETS * PER_BUCKET / 2
	}
	keys = make([]int, 0, prealloc)
	vals = make([]int, 0, prealloc)
	counter := int(0)
	for head := int(0); head < int(math.Pow(2, float64(HASH_BITS))); head++ {
		var entry, bucket int = 0, head
		for {
			entryAddr := bucket*BUCKET_SIZE + BUCKET_HEADER + entry*ENTRY_SIZE
			entryKey, _ := binary.Varint(ht.Buf[entryAddr+1 : entryAddr+11])
			entryVal, _ := binary.Varint(ht.Buf[entryAddr+11 : entryAddr+21])
			if ht.Buf[entryAddr] == 1 {
				counter++
				keys = append(keys, int(entryKey))
				vals = append(vals, int(entryVal))
				if counter == limit {
					return
				}
			} else if entryKey == 0 && entryVal == 0 {
				break
			}
			if entry++; entry == PER_BUCKET {
				entry = 0
				if bucket = ht.nextBucket(bucket); bucket == 0 {
					return
				}
			}
		}
	}
	return
}
