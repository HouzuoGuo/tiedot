/*
Data structure - static hash table.
Static hash table file only contains binary data, made of entries and buckets. Every entry has an integer key and value.
Hash table bucket contains a fixed (constant) number of entries. When a bucket becomes full, a new bucket is chained to
it in order to store more entries.
An integer key may have one or more values assigned to it. Every key-value combination is unique in a hash table.
*/
package data

import (
	"encoding/binary"
	"github.com/HouzuoGuo/tiedot/tdlog"
)

const (
	// Hash table file initial size & size growth (16 MBytes)
	HT_FILE_GROWTH = 16 * 1048576
	// Table entry size (validity - byte, key - uint64, value - uint64)
	ENTRY_SIZE = 1 + 8 + 8
	// Bucket header size (next chained bucket number - uint64)
	BUCKET_HEADER = 8
	// Number of entries per bucket
	PER_BUCKET = 12
	// Number of bits used for hashing
	HASH_BITS = 16
	// Size of bucket in bytes
	BUCKET_SIZE = BUCKET_HEADER + PER_BUCKET*ENTRY_SIZE
	// Number of buckets in the beginning (2 ^ HASH_BITS)
	INITIAL_BUCKETS = uint64(65536)
)

// Hash table file contains binary data of table entries and buckets.
type HashTable struct {
	*DataFile
	numBuckets uint64
}

/*
Smear (re-hash) the entry key for a better distribution in hash table.
Then return portion of the key used for hash table operation (last HASH_BITS bits).
*/
func HashKey(key uint64) uint64 {
	key = key ^ (key >> 4)
	key = (key ^ 0xdeadbeef) + (key << 5)
	key = key ^ (key >> 11)
	return key & ((1 << HASH_BITS) - 1) // retrieve the last N bits
}

// Open a hash table file.
func OpenHashTable(path string) (ht *HashTable, err error) {
	ht = &HashTable{}
	if ht.DataFile, err = OpenDataFile(path, HT_FILE_GROWTH); err != nil {
		return
	}
	ht.calculateNumBuckets()
	return
}

// Follow the longest bucket chain to calculate total number of buckets as well as the "used size" of data file.
func (ht *HashTable) calculateNumBuckets() {
	ht.numBuckets = ht.Size / BUCKET_SIZE
	largestBucketNum := INITIAL_BUCKETS - 1
	for i := uint64(0); i < INITIAL_BUCKETS; i++ {
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
	tdlog.Infof("%s: calculated used size is %d", ht.Path, usedSize)
}

// Return the bucket number of the next one in chain.
func (ht *HashTable) nextBucket(bucket uint64) uint64 {
	if bucket >= ht.numBuckets {
		return 0
	}
	bucketAddr := bucket * BUCKET_SIZE
	next := binary.LittleEndian.Uint64(ht.Buf[bucketAddr:])
	if next == 0 {
		return 0
	} else if next <= bucket || next >= ht.numBuckets || next < INITIAL_BUCKETS {
		tdlog.CritNoRepeat("Bad hash table - repair ASAP %s", ht.Path)
		return 0
	} else {
		return next
	}
}

// Return the bucket number of the last one in chain.
func (ht *HashTable) lastBucket(bucket uint64) uint64 {
	for curr := bucket; ; {
		next := ht.nextBucket(curr)
		if next == 0 {
			return curr
		}
		curr = next
	}
}

// Create a new bucket and put it into bucket chain.
func (ht *HashTable) growBucket(bucket uint64) {
	ht.EnsureSize(BUCKET_SIZE)
	lastBucketAddr := ht.lastBucket(bucket) * BUCKET_SIZE
	binary.LittleEndian.PutUint64(ht.Buf[lastBucketAddr:], ht.numBuckets)
	ht.Used += BUCKET_SIZE
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
func (ht *HashTable) Put(key, val uint64) {
	for bucket, entry := HashKey(key), uint64(0); ; {
		entryAddr := bucket*BUCKET_SIZE + BUCKET_HEADER + entry*ENTRY_SIZE
		if ht.Buf[entryAddr] != 1 {
			ht.Buf[entryAddr] = 1
			binary.LittleEndian.PutUint64(ht.Buf[entryAddr+1:], key)
			binary.LittleEndian.PutUint64(ht.Buf[entryAddr+9:], val)
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

// Look up values associated to the key.
func (ht *HashTable) Get(key, limit uint64) (vals []uint64) {
	if limit == 0 {
		vals = make([]uint64, 0, 16)
	} else {
		vals = make([]uint64, 0, limit)
	}
	for count, entry, bucket := uint64(0), uint64(0), HashKey(key); ; {
		entryAddr := bucket*BUCKET_SIZE + BUCKET_HEADER + entry*ENTRY_SIZE
		entryKey := binary.LittleEndian.Uint64(ht.Buf[entryAddr+1:])
		entryVal := binary.LittleEndian.Uint64(ht.Buf[entryAddr+9:])
		if ht.Buf[entryAddr] == 1 {
			if entryKey == key {
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
			if bucket = ht.nextBucket(bucket); bucket == 0 {
				return
			}
		}
	}
}

// Flag an entry as invalid, so that Get operation will not find it later on.
func (ht *HashTable) Remove(key, val uint64) {
	for entry, bucket := uint64(0), HashKey(key); ; {
		entryAddr := bucket*BUCKET_SIZE + BUCKET_HEADER + entry*ENTRY_SIZE
		entryKey := binary.LittleEndian.Uint64(ht.Buf[entryAddr+1:])
		entryVal := binary.LittleEndian.Uint64(ht.Buf[entryAddr+9:])
		if ht.Buf[entryAddr] == 1 {
			if entryKey == key && entryVal == val {
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

// Divide the entire hash table into roughly equally sized partitions, and return the start/end key range of the chosen partition.
func GetPartitionRange(partNum, totalParts uint64) (start uint64, end uint64) {
	perPart := INITIAL_BUCKETS / totalParts
	leftOver := INITIAL_BUCKETS % totalParts
	start = partNum * perPart
	if leftOver > 0 {
		if partNum == 0 {
			end += 1
		} else if partNum < leftOver {
			start += partNum
			end += 1
		} else {
			start += leftOver
		}
	}
	end += start + perPart
	if partNum == totalParts-1 {
		end = INITIAL_BUCKETS
	}
	return
}

// Collect all entries belonging to the specified bucket and all of its chained buckets.
func (ht *HashTable) collectEntries(head uint64) (keys, vals []uint64) {
	keys = make([]uint64, 0, PER_BUCKET)
	vals = make([]uint64, 0, PER_BUCKET)
	var entry, bucket uint64 = 0, head
	for {
		entryAddr := bucket*BUCKET_SIZE + BUCKET_HEADER + entry*ENTRY_SIZE
		entryKey := binary.LittleEndian.Uint64(ht.Buf[entryAddr+1:])
		entryVal := binary.LittleEndian.Uint64(ht.Buf[entryAddr+9:])
		if ht.Buf[entryAddr] == 1 {
			keys = append(keys, entryKey)
			vals = append(vals, entryVal)
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

// Divide the entire hash table into roughly equally sized partitions, and return all entries in the chosen partition.
func (ht *HashTable) GetPartition(partNum, partSize uint64) (keys, vals []uint64) {
	rangeStart, rangeEnd := GetPartitionRange(partNum, partSize)
	prealloc := (rangeEnd - rangeStart) * PER_BUCKET
	keys = make([]uint64, 0, prealloc)
	vals = make([]uint64, 0, prealloc)
	for head := rangeStart; head < rangeEnd; head++ {
		k, v := ht.collectEntries(head)
		keys = append(keys, k...)
		vals = append(vals, v...)
	}
	return
}
