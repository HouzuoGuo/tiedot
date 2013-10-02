/* Essentially a map[uint64]*sync.RWMutex, with more granularity. */

package file

import (
	"sync"
)

type MultiRWMutex struct {
	buckets     []map[uint64]*sync.RWMutex
	bucketLocks []*sync.RWMutex
	levels      uint64
}

func NewMultiRWMutex(levels int) *MultiRWMutex {
	mutexes := &MultiRWMutex{
		buckets:     make([]map[uint64]*sync.RWMutex, levels),
		bucketLocks: make([]*sync.RWMutex, levels),
		levels:      uint64(levels)}
	for i := 0; i < levels; i++ {
		mutexes.buckets[i] = make(map[uint64]*sync.RWMutex)
		mutexes.bucketLocks[i] = new(sync.RWMutex)
	}
	return mutexes
}

func (mutexes *MultiRWMutex) GetRWMutex(key uint64) *sync.RWMutex {
	// an intermediate step of MurmurHash3 (Austin Appleby, public domain)
	code := 0xcc9e2d51 * key
	bucketNumber := (0x1b873593 * (code<<15 | code>>15)) % mutexes.levels
	bucket := mutexes.buckets[bucketNumber]
	bucketLock := mutexes.bucketLocks[bucketNumber]

	var mutex *sync.RWMutex
	var ok bool
	bucketLock.RLock()
	mutex, ok = bucket[key]
	bucketLock.RUnlock()
	if !ok {
		bucketLock.Lock()
		bucket[key] = new(sync.RWMutex)
		bucketLock.Unlock()
		bucketLock.RLock()
		if mutex, ok = bucket[key]; !ok {
			panic("this should not happen, a lock was created but then disappeared")
		}
		bucketLock.RUnlock()
	}
	return mutex
}

func (mutexes *MultiRWMutex) LockAll() {
	for _, v := range mutexes.bucketLocks {
		v.Lock()
	}
	for _, bucket := range mutexes.buckets {
		for _, mutex := range bucket {
			mutex.Lock()
		}
	}
}

func (mutexes *MultiRWMutex) UnlockAll() {
	for _, v := range mutexes.bucketLocks {
		v.Unlock()
	}
	for _, bucket := range mutexes.buckets {
		for _, mutex := range bucket {
			mutex.Unlock()
		}
	}
}
