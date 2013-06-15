package db

import (
	"fmt"
)

const ()

func Eval(expr interface{}, src *Col) (result []uint64) {
	fmt.Println("Eval", expr)
	switch v := expr.(type) {
	case nil:
		return []uint64{}
	case uint64:
		return []uint64{v}
	case float64:
		return []uint64{uint64(v)}
	case []interface{}:
		result = make([]uint64, 0, 16)
		for _, sub := range v {
			result = append(result, Eval(sub, src)...)
		}
	case map[string]interface{}:
	}
	return result
}
