package file

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"os"
)

const (
	HASH_TABLE_GROWTH  = uint64(67108864) // Grows every 64MB
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
		return nil, errors.New(fmt.Sprintf("Invalid hash table parameter (%d hash bits, %d per bucket)", hashBits, perBucket))
	}
	file, err := Open(name, HASH_TABLE_GROWTH)
	if err != nil {
		return
	}
	ht = &HashTable{File: file, HashBits: hashBits, PerBucket: perBucket}
	ht.BucketSize = BUCKET_HEADER_SIZE + ENTRY_SIZE*perBucket
	// File has to be big enough to contain all initial buckets
	if minAppend := uint64(math.Pow(2, float64(hashBits))) * ht.BucketSize; ht.File.Append < minAppend {
		ht.File.Ensure(minAppend - ht.File.Append)
		ht.File.Append = minAppend
	}
	// Move append position to end of final bucket
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
		if next, _ := binary.Uvarint(ht.File.Buf[bucketAddr : bucketAddr+BUCKET_HEADER_SIZE]); next != 0 && next <= bucket {
			fmt.Fprintf(os.Stderr, "Hash table %s has a corrupted bucket %d which forms a loop at address %d\n", ht.File.Name, bucketAddr, bucket)
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
	lastBucketAddr := ht.LastBucket(bucket) * ht.BucketSize
	binary.PutUvarint(ht.File.Buf[lastBucketAddr:lastBucketAddr+8], ht.NumberBuckets())
	ht.File.Ensure(ht.BucketSize)
	ht.File.Append += ht.BucketSize
	fmt.Fprintf(os.Stderr, "Hash table %s has a grown a bucket from bucket %d\n", ht.File.Name, bucket)
}

// Return a hash key to be used by hash table by masking non-key bits.
func (ht *HashTable) HashKey(key uint64) uint64 {
	return key & ((1 << ht.HashBits) - 1)
}

// Put a new key-value pair.
func (ht *HashTable) Put(key, val uint64) {
	for bucket := ht.HashKey(key); ; {
		for entry, entryAddr := uint64(0), bucket*ht.BucketSize+BUCKET_HEADER_SIZE; entry < ht.PerBucket; entry, entryAddr = entry+1, BUCKET_HEADER_SIZE+bucket*ht.BucketSize+entry*ENTRY_SIZE {
			if ht.File.Buf[entryAddr] != ENTRY_VALID {
				ht.File.Buf[entryAddr] = ENTRY_VALID
				binary.PutUvarint(ht.File.Buf[entryAddr+1:entryAddr+9], key)
				binary.PutUvarint(ht.File.Buf[entryAddr+9:entryAddr+17], val)
				return
			}
		}
		if bucket = ht.NextBucket(bucket); bucket == 0 {
			break
		}
	}
	ht.grow(ht.HashKey(key))
	ht.Put(key, val)
}

// Get key-value pairs.
func (ht *HashTable) Get(key, limit uint64, filter func(uint64, uint64) bool) (keys, vals []uint64) {
	var count uint64 = 0
	if limit == 0 {
		keys = make([]uint64, 0, 10)
		vals = make([]uint64, 0, 10)
	} else {
		keys = make([]uint64, 0, limit)
		vals = make([]uint64, 0, limit)
	}
	for bucket := ht.HashKey(key); ; {
		for entry, entryAddr := uint64(0), bucket*ht.BucketSize+BUCKET_HEADER_SIZE; entry < ht.PerBucket; entry, entryAddr = entry+1, BUCKET_HEADER_SIZE+bucket*ht.BucketSize+entry*ENTRY_SIZE {
			entryKey, _ := binary.Uvarint(ht.File.Buf[entryAddr+1 : entryAddr+9])
			entryVal, _ := binary.Uvarint(ht.File.Buf[entryAddr+9 : entryAddr+17])
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
		}
		if bucket = ht.NextBucket(bucket); bucket == 0 {
			break
		}
	}
	return
}

// Remove specific key-value pair.
func (ht *HashTable) Remove(key, limit uint64, filter func(uint64, uint64) bool) {
	var count uint64 = 0

	for bucket := ht.HashKey(key); bucket != 0; bucket = ht.NextBucket(bucket) {
		for entry, entryAddr := uint64(0), uint64(0); entry < ht.PerBucket; entry, entryAddr = entry+1, bucket*ht.BucketSize+entry*ENTRY_SIZE {
			entryKey, _ := binary.Uvarint(ht.File.Buf[entryAddr+1 : entryAddr+9])
			entryVal, _ := binary.Uvarint(ht.File.Buf[entryAddr+9 : entryAddr+17])
			if ht.File.Buf[entryAddr] == ENTRY_VALID && entryKey == key && filter(entryKey, entryVal) {
				ht.File.Buf[entryAddr] = ENTRY_INVALID
				if count++; count == limit {
					return
				}
			} else if entryKey == 0 && entryVal == 0 {
				return
			}
		}
	}
}

// Return all entries in the hash table
func (ht *HashTable) GetAll() (keys, vals []uint64) {
	keys = make([]uint64, 1, ht.NumberBuckets()*ht.PerBucket/2)
	vals = make([]uint64, 1, ht.NumberBuckets()*ht.PerBucket/2)
	for curr := uint64(0); curr < uint64(math.Pow(2, float64(ht.HashBits))); curr++ {
	head:
		for chain := curr; ht.NextBucket(chain) != 0; chain = ht.NextBucket(curr) {
			for entry, entryAddr := uint64(0), uint64(0); entry < ht.PerBucket; entry, entryAddr = entry+1, chain*ht.BucketSize+entry*ENTRY_SIZE {
				entryKey, _ := binary.Uvarint(ht.File.Buf[entryAddr+1 : entryAddr+9])
				entryVal, _ := binary.Uvarint(ht.File.Buf[entryAddr+9 : entryAddr+17])
				if ht.File.Buf[entryAddr] == ENTRY_VALID {
					keys = append(keys, entryKey)
					vals = append(vals, entryVal)
				} else if entryKey == 0 && entryVal == 0 {
					break head
				}
			}
		}
	}
	return
}
