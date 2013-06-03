package main

import (
	"fmt"
	"loveoneanother.at/tiedot/file"
)

func main() {
	if col, err := file.OpenCol("/tmp/col"); err != nil {
		fmt.Println(err)
	} else {
		for i := 0; i < 10000000; i++ {
			col.Insert([]byte("abcde"))
		}
	}
}
