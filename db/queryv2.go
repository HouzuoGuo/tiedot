/* Query processor for embedded and HTTP API V2. */
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
			if err = EvalQueryV2(subExpr, src, result); err != nil {
				return
			}
		}
	case string:
		if expr == "all" {
			// put all IDs into result
			src.Data.ForAll(func(id uint64, _ []byte) bool {
				(*result)[id] = struct{}{}
				return true
			})
		} else {
			return errors.New(fmt.Sprintf("Do not know what %v means", expr))
		}
	case map[string]interface{}:
		// single query
		if lookupValue, lookup := expr["eq"]; lookup {
			// lookup
			path, hasPath := expr["in"]
			if !hasPath {
				return errors.New("Missing lookup path `in`")
			}
			// lookup path
			vecPath := make([]string, 0)
			if vecPathInterface, ok := path.([]interface{}); ok {
				for _, v := range vecPathInterface {
					vecPath = append(vecPath, fmt.Sprint(v))
				}
			} else {
				return errors.New(fmt.Sprintf("Expecting vector lookup path `in`, but %v given", path))
			}
			// result number limit
			intLimit := uint64(0)
			if limit, hasLimit := expr["limit"]; hasLimit {
				if floatLimit, ok := limit.(float64); ok {
					intLimit = uint64(floatLimit)
				} else {
					return errors.New(fmt.Sprintf("Expecting `limit` as a number, but %v given", limit))
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
							(*result)[id] = struct{}{}
							counter += 1
							return counter != intLimit
						}
					}
					return true
				})
			}
		} else if hasPath, exist := expr["has"]; exist {
			// value existence test
			vecPath := make([]string, 0)
			if vecPathInterface, ok := hasPath.([]interface{}); ok {
				for _, v := range vecPathInterface {
					vecPath = append(vecPath, fmt.Sprint(v))
				}
			} else {
				return errors.New(fmt.Sprintf("Expecting vector path, but %v given", hasPath))
			}
			intLimit := uint64(0)
			if limit, hasLimit := expr["limit"]; hasLimit {
				if floatLimit, ok := limit.(float64); ok {
					intLimit = uint64(floatLimit)
				} else {
					return errors.New(fmt.Sprintf("Expecting `limit` as a number, but %v given", limit))
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
						(*result)[id] = struct{}{}
						counter += 1
						return counter != intLimit
					}
					return true
				})
			}
		} else if subExprs, intersect := expr["n"]; intersect {
			// calculate intersection of sub-query results
			if subExprVecs, ok := subExprs.([]interface{}); ok {
				first := true
				for _, subExpr := range subExprVecs {
					subResult := make(map[uint64]struct{})
					intersection := make(map[uint64]struct{})
					if err = EvalQueryV2(subExpr, src, &subResult); err != nil {
						return
					}
					if first {
						*result = subResult
						first = false
					} else {
						for k, _ := range subResult {
							if _, inBoth := (*result)[k]; inBoth {
								intersection[k] = struct{}{}
							}
						}
						*result = intersection
					}
				}
			} else {
				return errors.New(fmt.Sprintf("Expecting a vector of sub-queries, but %v given", subExprs))
			}
		} else if subExprs, complement := expr["c"]; complement {
			// calculate complement of sub-query results
			if subExprVecs, ok := subExprs.([]interface{}); ok {
				for _, subExpr := range subExprVecs {
					subResult := make(map[uint64]struct{})
					complement := make(map[uint64]struct{})
					if err = EvalQueryV2(subExpr, src, &subResult); err != nil {
						return
					}
					for k, _ := range subResult {
						if _, inBoth := (*result)[k]; !inBoth {
							complement[k] = struct{}{}
						}
					}
					for k, _ := range *result {
						if _, inBoth := subResult[k]; !inBoth {
							complement[k] = struct{}{}
						}
					}
					*result = complement
				}
			} else {
				return errors.New(fmt.Sprintf("Expecting a vector of sub-queries, but %v given", subExprs))
			}
		} else if intFrom, htRange := expr["int-from"]; htRange {
			// range scan using hash table
			path, hasPath := expr["in"]
			if !hasPath {
				return errors.New("Missing path `in`")
			}
			// scan path
			vecPath := make([]string, 0)
			if vecPathInterface, ok := path.([]interface{}); ok {
				for _, v := range vecPathInterface {
					vecPath = append(vecPath, fmt.Sprint(v))
				}
			} else {
				return errors.New(fmt.Sprintf("Expecting vector path `in`, but %v given", path))
			}
			// result number limit
			intLimit := uint64(0)
			if limit, hasLimit := expr["limit"]; hasLimit {
				if floatLimit, ok := limit.(float64); ok {
					intLimit = uint64(floatLimit)
				} else {
					return errors.New(fmt.Sprintf("Expecting `limit` as a number, but %v given", limit))
				}
			}
			// from & to value
			from, to := uint64(0), uint64(0)
			if floatFrom, ok := intFrom.(float64); ok {
				from = uint64(floatFrom)
			} else {
				return errors.New(fmt.Sprintf("Expecting `int-from` as a number, but %v given", from))
			}
			if intTo, ok := expr["int-to"]; ok {
				if floatTo, ok := intTo.(float64); ok {
					to = uint64(floatTo)
				} else {
					return errors.New(fmt.Sprintf("Expecting `int-to` as a number, but %v given", to))
				}
			} else {
				return errors.New(fmt.Sprintf("Missing `int-to`"))
			}
			if from > to {
				tmp := to
				from = to
				to = tmp
			}
			if to-from > 1000 {
				log.Printf("Query %v involves hash table lookup of more than 1000 values, which is inefficient", q)
			}
			if ht, indexScan := src.StrHT[strings.Join(vecPath, ",")]; indexScan {
				counter := uint64(0)
				for lookupValue := from; lookupValue <= to; lookupValue++ {
					lookupStrValue := fmt.Sprint(lookupValue)
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
						if intLimit != 0 && counter == intLimit {
							break
						}
						counter += 1
						(*result)[docID] = struct{}{}
					}
				}
			} else {
				// fallback to collection scan
				log.Printf("Query %v involves collection scan, which is inefficient", q)
				counter := uint64(0)
				src.ForAll(func(id uint64, doc interface{}) bool {
					for _, v := range GetIn(doc, vecPath) {
						if floatV, ok := v.(float64); ok {
							if intV := uint64(floatV); intV <= to && intV >= from {
								(*result)[id] = struct{}{}
								counter += 1
								return counter != intLimit
							}
						}
					}
					return true
				})
			}
		} else {
			return errors.New(fmt.Sprintf("Query %v does not contain any operation (lookup/union/etc)", expr))
		}
	}
	return nil
}
