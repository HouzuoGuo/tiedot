package uuid

import (
	"crypto/rand"
	"encoding/hex"
)

func poolFiller(pool chan string) {
	for {
		uuid := make([]byte, 16)
		entropy, err := rand.Read(uuid)
		if err != nil {
			panic(err)
		}
		if entropy != 16 {
			panic("no enough entropy")
		}
		pool <- hex.EncodeToString(uuid)
	}
}

func UUIDPool() chan string {
	pool := make(chan string, 1000000)
	go poolFiller(pool)
	return pool
}
