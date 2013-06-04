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
	ENTRY_SIZE         = 1 + 8 + 8 // byte(validity), uint64(hash key), uint64(value)
	BUCKET_HEADER_SIZE = 8         // uint64(next bucket)
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

func (ht *HashTable) scan(key, limit uint64, proc func(int), filter func(int, int) bool) (keys, vals []uint64) {
	return
}
