package uid

import (
	"crypto/rand"
	"encoding/hex"
)

// Generate and return a new UID (Unique IDentifier).
func NextUID() string {
	uid := make([]byte, 16)
	// use Golang standard lib to get cryptographically secure pseudorandom numbers
	// the generator is backed by /dev/urandom (as per Golang's implementation)
	// be aware: this will be slow in Plan9 due to its lack of /dev/random
	entropy, err := rand.Read(uid)
	if err != nil || entropy != 16 {
		panic("no enough entropy")
	}
	return hex.EncodeToString(uid)
}
