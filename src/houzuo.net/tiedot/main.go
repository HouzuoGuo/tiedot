package main

import (
	"fmt"
	"houzuo.net/tiedot/data"
)

func main() {
	col, err := data.OpenCollection("/tmp/col", 100)
	if err != nil {
		fmt.Printf(err.Error())
	} else {
		col.Insert([]byte("abcde"))
	}
}
