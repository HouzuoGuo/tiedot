package uid

import (
	"crypto/rand"
	"encoding/hex"
)

func NextUID() string {
	uid := make([]byte, 16)
	entropy, err := rand.Read(uid)
	if err != nil || entropy != 16{
		panic("no enough entropy")
	}
	return hex.EncodeToString(uid)
}