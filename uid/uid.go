package uid

import "math/rand"

// Generate and return a new UID (Unique IDentifier).
func NextUID() uint64 {
	return uint64(rand.Int63()) + uint64(rand.Int63())
}
