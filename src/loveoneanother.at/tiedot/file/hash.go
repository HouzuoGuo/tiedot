/* A hash table of uint64 key-value pairs. */
package file

import (
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"math"
)

const (
	HASH_TABLE_GROWTH  = uint64(67108864) // Grows every 64MB
	ENTRY_VALID        = byte(1)
	ENTRY_INVALID      = byte(0)
	ENTRY_SIZE         = uint64(1 + 10 + 10) // byte(validity), uint64(hash key), uint64(value)
	BUCKET_HEADER_SIZE = uint64(10)          // uint64(next bucket)
)

type HashTable struct {
	File                            *File
	BucketSize, HashBits, PerBucket uint64
}

// Open a hash table file.
func OpenHash(name string, hashBits, perBucket uint64) (ht *HashTable, err error) {
	if hashBits < 1 || perBucket < 1 {
		return nil, errors.New(fmt.Sprintf("Invalid hash table parameter (%d hash bits, %d per bucket)", hashBits, perBucket))
	}
	file, err := Open(name, HASH_TABLE_GROWTH)
	if err != nil {
		return
	}
	ht = &HashTable{File: file, HashBits: hashBits, PerBucket: perBucket}
	ht.BucketSize = BUCKET_HEADER_SIZE + ENTRY_SIZE*perBucket
	// file has to be big enough to contain all initial buckets
	if minAppend := uint64(math.Pow(2, float64(hashBits))) * ht.BucketSize; ht.File.Append < minAppend {
		ht.File.Ensure(minAppend - ht.File.Append)
		ht.File.Append = minAppend
	}
	// move append position to end of final bucket
	if extra := ht.File.Append % ht.BucketSize; extra != 0 {
		ht.File.Append += ht.BucketSize - extra
	}
	return ht, nil
}

// Return total number of buckets.
func (ht *HashTable) numberBuckets() uint64 {
	return ht.File.Append / ht.BucketSize
}

// Return the number of next chained bucket.
func (ht *HashTable) nextBucket(bucket uint64) uint64 {
	if bucketAddr := bucket * ht.BucketSize; bucketAddr < 0 || bucketAddr > ht.File.Append {
		return 0
	} else {
		if next, _ := binary.Uvarint(ht.File.Buf[bucketAddr : bucketAddr+BUCKET_HEADER_SIZE]); next != 0 && next <= bucket {
			log.Printf("Loop detected in hash table %s at bucket %d, address %d\n", ht.File.Name, bucket, bucketAddr)
			return 0
		} else {
			return next
		}
	}
}

// Return the last bucket number in chain.
func (ht *HashTable) lastBucket(bucket uint64) uint64 {
	curr := bucket
	for {
		next := ht.nextBucket(curr)
		if next == 0 {
			return curr
		}
		curr = next
	}
}

// Grow a new bucket on the chain of buckets.
func (ht *HashTable) grow(bucket uint64) {
	lastBucketAddr := ht.lastBucket(bucket) * ht.BucketSize
	binary.PutUvarint(ht.File.Buf[lastBucketAddr:lastBucketAddr+8], ht.numberBuckets())
	ht.File.Ensure(ht.BucketSize)
	ht.File.Append += ht.BucketSize
}

// Return a hash key to be used by hash table by masking non-key bits.
func (ht *HashTable) hashKey(key uint64) uint64 {
	return key & ((1 << ht.HashBits) - 1)
}

// Put a new key-value pair.
func (ht *HashTable) Put(key, val uint64) {
	var bucket, entry uint64 = ht.hashKey(key), 0
	ht.File.Sync.Lock()
	for {
		entryAddr := bucket*ht.BucketSize + BUCKET_HEADER_SIZE + entry*ENTRY_SIZE
		if ht.File.Buf[entryAddr] != ENTRY_VALID {
			ht.File.Buf[entryAddr] = ENTRY_VALID
			binary.PutUvarint(ht.File.Buf[entryAddr+1:entryAddr+11], key)
			binary.PutUvarint(ht.File.Buf[entryAddr+11:entryAddr+21], val)
			ht.File.Sync.Unlock()
			return
		}
		if entry++; entry == ht.PerBucket {
			entry = 0
			if bucket = ht.nextBucket(bucket); bucket == 0 {
				ht.grow(ht.hashKey(key))
				ht.File.Sync.Unlock()
				ht.Put(key, val)
				return
			}
		}
	}
}

// Get key-value pairs.
func (ht *HashTable) Get(key, limit uint64, filter func(uint64, uint64) bool) (keys, vals []uint64) {
	var count, entry, bucket uint64 = 0, 0, ht.hashKey(key)
	if limit == 0 {
		keys = make([]uint64, 0, 10)
		vals = make([]uint64, 0, 10)
	} else {
		keys = make([]uint64, 0, limit)
		vals = make([]uint64, 0, limit)
	}
	ht.File.Sync.Lock()
	for {
		entryAddr := bucket*ht.BucketSize + BUCKET_HEADER_SIZE + entry*ENTRY_SIZE
		entryKey, _ := binary.Uvarint(ht.File.Buf[entryAddr+1 : entryAddr+11])
		entryVal, _ := binary.Uvarint(ht.File.Buf[entryAddr+11 : entryAddr+21])
		if ht.File.Buf[entryAddr] == ENTRY_VALID {
			if entryKey == key && filter(entryKey, entryVal) {
				keys = append(keys, entryKey)
				vals = append(vals, entryVal)
				if count++; count == limit {
					ht.File.Sync.Unlock()
					return
				}
			}
		} else if entryKey == 0 && entryVal == 0 {
			ht.File.Sync.Unlock()
			return
		}
		if entry++; entry == ht.PerBucket {
			entry = 0
			if bucket = ht.nextBucket(bucket); bucket == 0 {
				ht.File.Sync.Unlock()
				return
			}
		}
	}
}

// Remove specific key-value pair.
func (ht *HashTable) Remove(key, limit uint64, filter func(uint64, uint64) bool) {
	var count, entry, bucket uint64 = 0, 0, ht.hashKey(key)
	ht.File.Sync.Lock()
	for {
		entryAddr := bucket*ht.BucketSize + BUCKET_HEADER_SIZE + entry*ENTRY_SIZE
		entryKey, _ := binary.Uvarint(ht.File.Buf[entryAddr+1 : entryAddr+11])
		entryVal, _ := binary.Uvarint(ht.File.Buf[entryAddr+11 : entryAddr+21])
		if ht.File.Buf[entryAddr] == ENTRY_VALID {
			if entryKey == key && filter(entryKey, entryVal) {
				ht.File.Buf[entryAddr] = ENTRY_INVALID
				if count++; count == limit {
					ht.File.Sync.Unlock()
					return
				}
			}
		} else if entryKey == 0 && entryVal == 0 {
			ht.File.Sync.Unlock()
			return
		}
		if entry++; entry == ht.PerBucket {
			entry = 0
			if bucket = ht.nextBucket(bucket); bucket == 0 {
				ht.File.Sync.Unlock()
				return
			}
		}
	}
}

// Return all entries in the hash table.
func (ht *HashTable) GetAll() (keys, vals []uint64) {
	keys = make([]uint64, 0, ht.numberBuckets()*ht.PerBucket/2)
	vals = make([]uint64, 0, ht.numberBuckets()*ht.PerBucket/2)
	ht.File.Sync.Lock()
	for head := uint64(0); head < uint64(math.Pow(2, float64(ht.HashBits))); head++ {
		var entry, bucket uint64 = 0, head
		for {
			entryAddr := bucket*ht.BucketSize + BUCKET_HEADER_SIZE + entry*ENTRY_SIZE
			entryKey, _ := binary.Uvarint(ht.File.Buf[entryAddr+1 : entryAddr+11])
			entryVal, _ := binary.Uvarint(ht.File.Buf[entryAddr+11 : entryAddr+21])
			if ht.File.Buf[entryAddr] == ENTRY_VALID {
				keys = append(keys, entryKey)
				vals = append(vals, entryVal)
			} else if entryKey == 0 && entryVal == 0 {
				break
			}
			if entry++; entry == ht.PerBucket {
				entry = 0
				if bucket = ht.nextBucket(bucket); bucket == 0 {
					ht.File.Sync.Unlock()
					return
				}
			}
		}
	}
	ht.File.Sync.Unlock()
	return
}
