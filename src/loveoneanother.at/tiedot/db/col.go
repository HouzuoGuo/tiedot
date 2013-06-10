package db

import (
	"loveoneanother.at/tiedot/file"
)

type Col struct {
	data    *file.ColFile
	indexes map[string]*file.HashTable
}
