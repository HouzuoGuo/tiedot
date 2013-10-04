package uid

import (
	"fmt"
	"testing"
	"time"
)

func TestMiniUIDPool(t *testing.T) {
	pool := MiniUIDPool()
	next := <-pool
	fmt.Println(next)
	next = <-pool
	fmt.Println(next)
	next = <-pool
	fmt.Println(next)
	fmt.Println(len(pool))
	if len(next) != 32 {
		t.Fatalf("malformed uid")
	}
	if len(pool) < 10 {
		t.Fatalf("not enough uid in pool")
	}
}

func TestRegularUIDPool(t *testing.T) {
	pool := UIDPool()
	next := <-pool
	fmt.Println(next)
	next = <-pool
	fmt.Println(next)
	next = <-pool
	fmt.Println(next)
	fmt.Println(len(pool))
	if len(next) != 32 {
		t.Fatalf("malformed uid")
	}
	if len(pool) < 10 {
		t.Fatalf("not enough uid in pool")
	}
}

func BenchmarkUID(b *testing.B) {
	pool := UIDPool()
	fmt.Println("Waiting 10 seconds, to fill up intiial pool")
	time.Sleep(10 * time.Second)
	fmt.Println("UID pool is ready with ", len(pool), " UIDs")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if i%5000 == 0 {
			if len(pool) < 5000 {
				fmt.Println("Pool exhausted, only ", len(pool), " UIDs remaining. Now waiting another 10 seconds for refilling")
				b.StopTimer()
				time.Sleep(10 * time.Second)
				fmt.Println("Pool is refilled with ", len(pool), " UIDs remaining")
				b.StartTimer()
			}
		}
		<-pool
	}
}
