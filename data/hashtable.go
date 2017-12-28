// Hash table file contains binary content.
//
// This package implements a static hash table made of hash buckets and integer
// entries.
//
// Every bucket has a fixed number of entries. When a bucket becomes full, a new
// bucket is chained to it in order to store more entries. Every entry has an
// integer key and value. An entry key may have multiple values assigned to it,
// however the combination of entry key and value must be unique across the
// entire hash table.

package data

import (
	"encoding/binary"
	"sync"

	"github.com/HouzuoGuo/tiedot/tdlog"
)

// Hash table file is a binary file containing buckets of hash entries.
type HashTable struct {
	*Config
	*DataFile
	numBuckets int
	Lock       *sync.RWMutex
}

// Open a hash table file.
func (conf *Config) OpenHashTable(path string) (ht *HashTable, err error) {
	ht = &HashTable{Config: conf, Lock: new(sync.RWMutex)}
	if ht.DataFile, err = OpenDataFile(path, ht.HTFileGrowth); err != nil {
		return
	}
	conf.CalculateConfigConstants()
	ht.calculateNumBuckets()
	return
}

// Follow the longest bucket chain to calculate total number of buckets, hence the "used size" of hash table file.
func (ht *HashTable) calculateNumBuckets() {
	ht.numBuckets = ht.Size / ht.BucketSize
	largestBucketNum := ht.InitialBuckets - 1
	for i := 0; i < ht.InitialBuckets; i++ {
		lastBucket := ht.lastBucket(i)
		if lastBucket > largestBucketNum && lastBucket < ht.numBuckets {
			largestBucketNum = lastBucket
		}
	}
	ht.numBuckets = largestBucketNum + 1
	usedSize := ht.numBuckets * ht.BucketSize
	if usedSize > ht.Size {
		ht.Used = ht.Size
		ht.EnsureSize(usedSize - ht.Used)
	}
	ht.Used = usedSize
	tdlog.Infof("%s: calculated used size is %d", ht.Path, usedSize)
}

// Return number of the next chained bucket.
func (ht *HashTable) nextBucket(bucket int) int {
	if bucket >= ht.numBuckets {
		return 0
	}
	bucketAddr := bucket * ht.BucketSize
	nextUint, err := binary.Varint(ht.Buf[bucketAddr : bucketAddr+10])
	next := int(nextUint)
	if next == 0 {
		return 0
	} else if err < 0 || next <= bucket || next >= ht.numBuckets || next < ht.InitialBuckets {
		tdlog.CritNoRepeat("Bad hash table - repair ASAP %s", ht.Path)
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

// Create and chain a new bucket.
func (ht *HashTable) growBucket(bucket int) {
	ht.EnsureSize(ht.BucketSize)
	lastBucketAddr := ht.lastBucket(bucket) * ht.BucketSize
	binary.PutVarint(ht.Buf[lastBucketAddr:lastBucketAddr+10], int64(ht.numBuckets))
	ht.Used += ht.BucketSize
	ht.numBuckets++
}

// Clear the entire hash table.
func (ht *HashTable) Clear() (err error) {
	if err = ht.DataFile.Clear(); err != nil {
		return
	}
	ht.calculateNumBuckets()
	return
}

// Store the entry into a vacant (invalidated or empty) place in the appropriate bucket.
func (ht *HashTable) Put(key, val int) {
	for bucket, entry := ht.HashKey(key), 0; ; {
		entryAddr := bucket*ht.BucketSize + BucketHeader + entry*EntrySize
		if ht.Buf[entryAddr] != 1 {
			ht.Buf[entryAddr] = 1
			binary.PutVarint(ht.Buf[entryAddr+1:entryAddr+11], int64(key))
			binary.PutVarint(ht.Buf[entryAddr+11:entryAddr+21], int64(val))
			return
		}
		if entry++; entry == ht.PerBucket {
			entry = 0
			if bucket = ht.nextBucket(bucket); bucket == 0 {
				ht.growBucket(ht.HashKey(key))
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
	for count, entry, bucket := 0, 0, ht.HashKey(key); ; {
		entryAddr := bucket*ht.BucketSize + BucketHeader + entry*EntrySize
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
		if entry++; entry == ht.PerBucket {
			entry = 0
			if bucket = ht.nextBucket(bucket); bucket == 0 {
				return
			}
		}
	}
}

// Flag an entry as invalid, so that Get will not return it later on.
func (ht *HashTable) Remove(key, val int) {
	for entry, bucket := 0, ht.HashKey(key); ; {
		entryAddr := bucket*ht.BucketSize + BucketHeader + entry*EntrySize
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
		if entry++; entry == ht.PerBucket {
			entry = 0
			if bucket = ht.nextBucket(bucket); bucket == 0 {
				return
			}
		}
	}
}

// Divide the entire hash table into roughly equally sized partitions, and return the start/end key range of the chosen partition.
func (conf *Config) GetPartitionRange(partNum, totalParts int) (start int, end int) {
	perPart := conf.InitialBuckets / totalParts
	leftOver := conf.InitialBuckets % totalParts
	start = partNum * perPart
	if leftOver > 0 {
		if partNum == 0 {
			end++
		} else if partNum < leftOver {
			start += partNum
			end++
		} else {
			start += leftOver
		}
	}
	end += start + perPart
	if partNum == totalParts-1 {
		end = conf.InitialBuckets
	}
	return
}

// Collect entries all the way from "head" bucket to the end of its chained buckets.
func (ht *HashTable) collectEntries(head int) (keys, vals []int) {
	keys = make([]int, 0, ht.PerBucket)
	vals = make([]int, 0, ht.PerBucket)
	var entry, bucket int = 0, head
	for {
		entryAddr := bucket*ht.BucketSize + BucketHeader + entry*EntrySize
		entryKey, _ := binary.Varint(ht.Buf[entryAddr+1 : entryAddr+11])
		entryVal, _ := binary.Varint(ht.Buf[entryAddr+11 : entryAddr+21])
		if ht.Buf[entryAddr] == 1 {
			keys = append(keys, int(entryKey))
			vals = append(vals, int(entryVal))
		} else if entryKey == 0 && entryVal == 0 {
			return
		}
		if entry++; entry == ht.PerBucket {
			entry = 0
			if bucket = ht.nextBucket(bucket); bucket == 0 {
				return
			}
		}
	}
}

// Return all entries in the chosen partition.
func (ht *HashTable) GetPartition(partNum, partSize int) (keys, vals []int) {
	rangeStart, rangeEnd := ht.GetPartitionRange(partNum, partSize)
	prealloc := (rangeEnd - rangeStart) * ht.PerBucket
	keys = make([]int, 0, prealloc)
	vals = make([]int, 0, prealloc)
	for head := rangeStart; head < rangeEnd; head++ {
		k, v := ht.collectEntries(head)
		keys = append(keys, k...)
		vals = append(vals, v...)
	}
	return
}
