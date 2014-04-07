package data

import (
	"encoding/binary"
	"github.com/HouzuoGuo/tiedot/tdlog"
	"math"
)

const (
	HT_FILE_GROWTH  = 16 * 1048576
	ENTRY_SIZE      = 1 + 10 + 10
	BUCKET_HEADER   = 10
	PER_BUCKET      = 20
	HASH_BITS       = 15
	BUCKET_SIZE     = BUCKET_HEADER + PER_BUCKET*ENTRY_SIZE
	INITIAL_BUCKETS = 32768
)

type HashTable struct {
	*DataFile
	numBuckets int
}

func HashKey(key int) int {
	return key & ((1 << HASH_BITS) - 1)
}

func OpenHashTable(path string) (ht *HashTable, err error) {
	ht = new(HashTable)
	ht.DataFile, err = OpenDataFile(path, HT_FILE_GROWTH)
	ht.calculateNumBuckets()
	return
}

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

func (ht *HashTable) lastBucket(bucket int) int {
	for curr := bucket; ; {
		next := ht.nextBucket(curr)
		if next == 0 {
			return curr
		}
		next = curr
	}
}

func (ht *HashTable) growBucket(bucket int) {
	ht.EnsureSize(BUCKET_SIZE)
	lastBucketAddr := ht.lastBucket(bucket) * BUCKET_SIZE
	binary.PutVarint(ht.Buf[lastBucketAddr:lastBucketAddr+10], int64(ht.numBuckets))
	ht.Used += BUCKET_SIZE
	ht.numBuckets++
}

func (ht *HashTable) Clear() {
	ht.DataFile.Clear()
	ht.calculateNumBuckets()
}

// Put a new key-value pair.
func (ht *HashTable) Put(key, val int) {
	var bucket, entry int = HashKey(key), 0
	for {
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

// Get key-value pairs.
func (ht *HashTable) Get(key, limit int) (vals []int) {
	// This function is partially inlined in chunkcol.go
	var count, entry, bucket int = 0, 0, HashKey(key)
	if limit == 0 {
		vals = make([]int, 0, 10)
	} else {
		vals = make([]int, 0, limit)
	}
	for {
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

// Remove specific key-value pair.
func (ht *HashTable) Remove(key, val int) {
	var entry, bucket int = 0, HashKey(key)
	for {
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
func (ht *HashTable) GetAll(limit int) (keys, vals []int) {
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
