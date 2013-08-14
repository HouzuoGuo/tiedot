/* Query processor for API service version 2. */
package db

import (
	"errors"
	"fmt"
	"log"
	"strings"
)

// Evaluate a query and return query result (as map keys).
func EvalQueryV2(q interface{}, src *Col, result *map[uint64]struct{}) (err error) {
	switch expr := q.(type) {
	case float64:
		// single document number
		(*result)[uint64(expr)] = struct{}{}
	case []interface{}:
		// [sub query 1, sub query 2, etc]
		for _, subExpr := range expr {
			EvalQueryV2(subExpr, src, result)
		}
	case map[string]interface{}:
		// single query
		if lookupValue, lookup := expr["eq"]; lookup {
			// lookup
			path, hasPath := expr["in"]
			if !hasPath {
				return errors.New("Expecting value lookup path (`in`)")
			}
			// lookup path
			vecPath := make([]string, 0)
			if vecPathInterface, ok := path.([]interface{}); ok {
				for _, v := range vecPathInterface {
					vecPath = append(vecPath, fmt.Sprint(v))
				}
			} else {
				return errors.New(fmt.Sprintf("Expecting value lookup path (`in`) as a vector, but you gave me: %v", path))
			}
			// result number limit
			intLimit := uint64(0)
			if limit, hasLimit := expr["limit"]; hasLimit {
				if floatLimit, ok := limit.(float64); ok {
					intLimit = uint64(floatLimit)
				} else {
					return errors.New(fmt.Sprintf("Expecting `limit` as a number, but you gave me: %v", limit))
				}
			}
			lookupStrValue := fmt.Sprint(lookupValue)
			if ht, indexScan := src.StrHT[strings.Join(vecPath, ",")]; indexScan {
				// index scan is much prefered
				hashValue := StrHash(lookupStrValue)
				_, vals := ht.Get(hashValue, intLimit, func(k, v uint64) bool {
					// to avoid hash collision
					var doc interface{}
					// skip corrupted/incorrect/deleted document
					if src.Read(v, &doc) != nil {
						return false
					}
					for _, v := range GetIn(doc, vecPath) {
						if fmt.Sprint(v) == lookupStrValue {
							return true
						}
					}
					return false
				})
				for _, docID := range vals {
					(*result)[docID] = struct{}{}
				}
			} else {
				// fallback to collection scan
				log.Printf("Query %v involves collection scan, which is inefficient", q)
				counter := uint64(0)
				src.ForAll(func(id uint64, doc interface{}) bool {
					for _, v := range GetIn(doc, vecPath) {
						if fmt.Sprint(v) == lookupStrValue {
							counter += 1
							(*result)[id] = struct{}{}
							return counter != intLimit
						}
					}
					return true
				})
			}
		} else if existPath, exist := expr["exist"]; exist {
			// value existence test
			vecPath := make([]string, 0)
			if vecPathInterface, ok := existPath.([]interface{}); ok {
				for _, v := range vecPathInterface {
					vecPath = append(vecPath, fmt.Sprint(v))
				}
			} else {
				return errors.New(fmt.Sprintf("Expecting path as a vector, but you gave me: %v", existPath))
			}
			intLimit := uint64(0)
			if limit, hasLimit := expr["limit"]; hasLimit {
				if floatLimit, ok := limit.(float64); ok {
					intLimit = uint64(floatLimit)
				} else {
					return errors.New(fmt.Sprintf("Expecting `limit` as a number, but you gave me: %v", limit))
				}
			}
			// hash GetAll is *sometimes* faster than collection scan
			if ht, indexScan := src.StrHT[strings.Join(vecPath, ",")]; indexScan {
				// hash scan
				_, vals := ht.GetAll(intLimit)
				for _, docID := range vals {
					(*result)[docID] = struct{}{}
				}
			} else {
				// collection scan
				counter := uint64(0)
				src.ForAll(func(id uint64, doc interface{}) bool {
					vals := GetIn(doc, vecPath)
					if !(vals == nil || len(vals) == 1 && vals[0] == nil) {
						counter += 1
						(*result)[id] = struct{}{}
						return counter != intLimit
					}
					return true
				})
			}
		}
	}
	return nil
}
