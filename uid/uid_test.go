package uid

import "testing"

func TestNextUID(t *testing.T) {
	for i := 0; i < 1000; i++ {
		next := NextUID()
		if next == 0 {
			t.Fatal(next)
		}
	}
}
