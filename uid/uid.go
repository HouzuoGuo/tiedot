package uid

import (
	"crypto/rand"
	"encoding/hex"
)

func poolFiller(pool chan string) {
	for {
		uid := make([]byte, 16)
		entropy, err := rand.Read(uid)
		if err != nil {
			panic(err)
		}
		if entropy != 16 {
			panic("no enough entropy")
		}
		pool <- hex.EncodeToString(uid)
	}
}

func UIDPool() chan string {
	pool := make(chan string, 1000000)
	go poolFiller(pool)
	return pool
}
