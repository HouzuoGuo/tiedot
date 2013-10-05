package uid

import (
	"fmt"
	"testing"
)

func TestNextUID(t *testing.T) {
	next := NextUID()
	fmt.Println(next)
	next = NextUID()
	fmt.Println(next)
	next = NextUID()
	fmt.Println(next)
	if len(next) != 32 {
		t.Fatalf("malformed uid")
	}
}

func BenchmarkUID(b *testing.B) {
	for i := 0; i < b.N; i++ {
		NextUID()
	}
}
