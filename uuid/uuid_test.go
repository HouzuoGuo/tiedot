package uuid

import (
	"fmt"
	"testing"
	"time"
)

func TestUUID(t *testing.T) {
	pool := UUIDPool()
	next := <-pool
	fmt.Println(next)
	next = <-pool
	fmt.Println(next)
	next = <-pool
	fmt.Println(next)
	fmt.Println(len(pool))
	if len(next) < 16 {
		t.Fatalf("malformed uuid")
	}
	if len(pool) < 10 {
		t.Fatalf("not enough uuid in pool")
	}
}

func BenchmarkUUID(b *testing.B) {
	pool := UUIDPool()
	fmt.Println("Waiting 10 seconds, to fill up intiial pool")
	time.Sleep(10 * time.Second)
	fmt.Println("UUID pool is ready with ", len(pool), " UUIDs")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if i%5000 == 0 {
			if len(pool) < 5000 {
				fmt.Println("Pool exhausted, only ", len(pool), " UUIDs remaining. Now waiting another 10 seconds for refilling")
				b.StopTimer()
				time.Sleep(10 * time.Second)
				fmt.Println("Pool is refilled with ", len(pool), " UUIDs remaining")
				b.StartTimer()
			}
		}
		<-pool
	}
}
