package file

import (
	"sync"
	"testing"
)

func TestMultiRWMutex(t *testing.T) {
	mutexes := NewMultiRWMutex(100)
	wg := new(sync.WaitGroup)
	wg.Add(32)
	for i := 0; i < 32; i++ {
		go func() {
			defer wg.Done()
			for i := uint64(0); i < 40000; i++ {
				mutexes.GetRWMutex(i)
			}
		}()
	}
	wg.Wait()
	mutexes.LockAll()
	mutexes.UnlockAll()
}
