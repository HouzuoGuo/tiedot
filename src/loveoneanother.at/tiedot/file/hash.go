package file

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"os"
)

const (
	HASH_TABLE_GROWTH  = uint64(134217728) // Grows every 128MB
	ENTRY_VALID        = byte(1)
	ENTRY_INVALID      = byte(0)
	ENTRY_SIZE         = uint64(1 + 8 + 8) // byte(validity), uint64(hash key), uint64(value)
	BUCKET_HEADER_SIZE = uint64(8)         // uint64(next bucket)
)

type HashTable struct {
	File                            *File
	BucketSize, HashBits, PerBucket uint64
}

func StrHash(str string) int {
	length := len(str)
	hash := int(0)
	for i, c := range str {
		hash += int(c)*31 ^ (length - i)
	}
	return hash
}

// Open a hash table file.
func OpenHash(name string, hashBits, perBucket uint64) (ht *HashTable, err error) {
	if hashBits < 1 || perBucket < 1 {
		return nil, errors.New(fmt.Sprintf("Invalid hash table parameter (%d hash bits, %d per bucket)\n", hashBits, perBucket))
	}
	file, err := Open(name, HASH_TABLE_GROWTH)
	if err != nil {
		return
	}
	ht = &HashTable{File: file, HashBits: hashBits, PerBucket: perBucket}
	ht.BucketSize = BUCKET_HEADER_SIZE + uint64(math.Pow(2, float64(hashBits)))*perBucket*ENTRY_SIZE
	// Fix append position (to be end of last bucket)
	if extra := ht.File.Append % ht.BucketSize; extra != 0 {
		ht.File.Append += ht.BucketSize - extra
	}
	return ht, nil
}

// Return total number of buckets.
func (ht *HashTable) NumberBuckets() uint64 {
	return ht.File.Append / ht.BucketSize
}

// Return the number of next chained bucket.
func (ht *HashTable) NextBucket(bucket uint64) uint64 {
	if bucketAddr := bucket * ht.BucketSize; bucketAddr < 0 || bucketAddr > ht.File.Append {
		return 0
	} else {
		if next, _ := binary.Uvarint(ht.File.Buf[bucketAddr : bucketAddr+BUCKET_HEADER_SIZE]); next <= bucket {
			fmt.Fprintf(os.Stderr, "Hash table %s has a (corrupted) bucket %d which forms a loop at address %d\n", ht.File.Name, bucketAddr, bucket)
			return 0
		} else {
			return next
		}
	}
}

// Return the last bucket number in chain.
func (ht *HashTable) LastBucket(bucket uint64) uint64 {
	curr := bucket
	for ; ht.NextBucket(curr) != 0; curr = ht.NextBucket(curr) {
	}
	return curr
}

// Grow a new bucket on the chain of buckets.
func (ht *HashTable) grow(bucket uint64) {
	fmt.Fprintf(os.Stderr, "Hash table %s has a grown a bucket from bucket %d %d\n", ht.File.Name, bucket)
	lastBucketAddr := ht.LastBucket(bucket) * ht.BucketSize
	binary.PutUvarint(ht.File.Buf[lastBucketAddr:lastBucketAddr+8], ht.NumberBuckets())
	ht.File.Ensure(ht.BucketSize)
	ht.File.Append += ht.BucketSize
}

// Return a hash key to be used by hash table by masking non-key bits.
func (ht *HashTable) HashKey(key uint64) uint64 {
	return key & ((1 << ht.HashBits) - 1)
}

// Put a new key-value pair.
func (ht *HashTable) Put(key, val uint64) {
	for bucket := ht.HashKey(key); bucket != 0; bucket = ht.NextBucket(bucket) {
		for entry, entryAddr := uint64(0), uint64(0); entry < ht.PerBucket; entry, entryAddr = entry+1, bucket*ht.BucketSize+entry*ENTRY_SIZE {
			if ht.File.Buf[entryAddr] == ENTRY_INVALID {
				ht.File.Buf[entryAddr] = ENTRY_VALID
				binary.PutUvarint(ht.File.Buf[entryAddr+1:entryAddr+9], key)
				binary.PutUvarint(ht.File.Buf[entryAddr+9:entryAddr+18], key)
				return
			}
		}
	}
	ht.grow(ht.HashKey(key))
	ht.Put(key, val)
}

// Get key-value pairs.
func (ht *HashTable) Get(key, limit uint64, filter func(uint64, uint64) bool) (keys, vals []uint64) {
	var count uint64 = 0
	if limit == 0 {
		keys = make([]uint64, 1, 100)
		vals = make([]uint64, 1, 100)
	} else {
		keys = make([]uint64, 1, limit)
		vals = make([]uint64, 1, limit)
	}
	for bucket := ht.HashKey(key); bucket != 0; bucket = ht.NextBucket(bucket) {
		for entry, entryAddr := uint64(0), uint64(0); entry < ht.PerBucket; entry, entryAddr = entry+1, bucket*ht.BucketSize+entry*ENTRY_SIZE {
			if ht.File.Buf[entryAddr] == ENTRY_VALID {
				if entryKey, _ := binary.Uvarint(ht.File.Buf[entryAddr+1 : entryAddr+9]); entryKey == key {
					if val, _ := binary.Uvarint(ht.File.Buf[entryAddr+9 : entryAddr+18]); filter(key, val) {
						keys = append(keys, entryKey)
						vals = append(keys, val)
						count++
						if count == limit {
							return
						}
					}
				}
			}
		}
	}
	return
}

// Remove specific key-value pair.
func (ht *HashTable) Remove(key, limit uint64, filter func(uint64, uint64) bool) {
	var count uint64 = 0
	for bucket := ht.HashKey(key); bucket != 0; bucket = ht.NextBucket(bucket) {
		for entry, entryAddr := uint64(0), uint64(0); entry < ht.PerBucket; entry, entryAddr = entry+1, bucket*ht.BucketSize+entry*ENTRY_SIZE {
			if ht.File.Buf[entryAddr] == ENTRY_VALID {
				if entryKey, _ := binary.Uvarint(ht.File.Buf[entryAddr+1 : entryAddr+9]); entryKey == key {
					if val, _ := binary.Uvarint(ht.File.Buf[entryAddr+9 : entryAddr+18]); filter(key, val) {
						ht.File.Buf[entryAddr] = ENTRY_INVALID
						count++
						if count == limit {
							return
						}
					}
				}
			}
		}
	}
}

// Return all entries in the hash table
func (ht *HashTable) GetAll() (keys, vals []uint64) {
	keys = make([]uint64, 1, ht.NumberBuckets()*ht.PerBucket/2)
	vals = make([]uint64, 1, ht.NumberBuckets()*ht.PerBucket/2)
	for curr := uint64(0); curr < uint64(math.Pow(2, float64(ht.HashBits))); curr++ {
		for chain := curr; ht.NextBucket(chain) != 0; chain = ht.NextBucket(curr) {
			for entry := 0; entry < ht.PerBucket; entry++ {
			}
		}
	}
	return
}
