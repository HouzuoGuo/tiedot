package main

import (
	"fmt"
	"loveoneanother.at/tiedot/file"
	"os"
)

func main() {
	tmp := "/tmp/tiedot_hash_test"
	os.Remove(tmp)
	//	defer os.Remove(tmp)
	ht, err := file.OpenHash(tmp, 3, 3)
	if err != nil {
		return
	}
	for i := uint64(0); i < 20; i++ {
		ht.Put(i, i)
	}
	fmt.Println("Put completed")
	for i := uint64(0); i < 20; i++ {
		keys, vals := ht.Get(i, 1, func(a, b uint64) bool {
			return true
		})
		if !(cap(keys) == 1 && keys[0] == i && cap(vals) == 1 && vals[0] == i) {
			fmt.Println("get failed", i)
		}
	}
	ht.File.Close()
}
