/* Query processor. */
package db

import (
	"errors"
	"fmt"
	"log"
	"strings"
)

// Evaluate a query and return query result (as map keys).
func EvalQuery(q interface{}, src *Col, result *map[uint64]bool) (err error) {
	switch expr := q.(type) {
	// 1, 2.0, etc
	case float64:
		(*result)[uint64(expr)] = false
	// ["op", "param1", "param2"...]
	case []interface{}:
		switch op := expr[0].(type) {
		// "op"
		case string:
			switch op {
			// value existence
			case "exist":
				switch lookupParam := expr[1].(type) {
				case map[string]interface{}:
					limit, hasLimit := lookupParam["limit"]
					path, hasPath := lookupParam["in"]
					if !hasPath {
						err = errors.New(fmt.Sprintf("Expecting `in` vector"))
						return
					}
					vecPath := make([]string, 0)
					intLimit := uint64(0)
					// figure out lookup path
					if vecIfPath, ok := path.([]interface{}); ok {
						for _, v := range vecIfPath {
							vecPath = append(vecPath, fmt.Sprint(v))
						}
					} else {
						return errors.New(fmt.Sprintf("Expecting `in` vector, but you gave me: %v", path))

					}
					// figure out limit
					if hasLimit {
						if floatLimit, ok := limit.(float64); ok {
							intLimit = uint64(floatLimit)
						} else {
							return errors.New(fmt.Sprintf("Expecting `limit` number, but you gave me: %v", floatLimit))
						}
					}
					// hash GetAll is *sometimes* faster than collection scan
					if ht, indexScan := src.StrHT[strings.Join(vecPath, ",")]; indexScan {
						// hash scan
						_, vals := ht.GetAll(intLimit)
						for _, docID := range vals {
							(*result)[docID] = false
						}
					} else {
						// collection scan
						counter := uint64(0)
						src.ForAll(func(id uint64, doc interface{}) bool {
							vals := GetIn(doc, vecPath)
							if !(vals == nil || len(vals) == 1 && vals[0] == nil) {
								counter += 1
								(*result)[id] = false
								return counter != intLimit
							}
							return true
						})
					}
				default:
					return errors.New(fmt.Sprintf("Expecting a map of lookup parameters, but you gave me: %v", lookupParam))
				}
			// lookup
			case "=":
				switch lookupParam := expr[1].(type) {
				case map[string]interface{}:
					limit, hasLimit := lookupParam["limit"]
					path, hasPath := lookupParam["in"]
					lookupValue, hasEq := lookupParam["eq"]
					if !(hasPath && hasEq) {
						err = errors.New(fmt.Sprintf("Expecting `in` vector and `eq` value"))
						return
					}
					vecPath := make([]string, 0)
					intLimit := uint64(0)
					// figure out lookup path
					if vecIfPath, ok := path.([]interface{}); ok {
						for _, v := range vecIfPath {
							vecPath = append(vecPath, fmt.Sprint(v))
						}
					} else {
						return errors.New(fmt.Sprintf("Expecting `in` vector, but you gave me: %v", path))

					}
					// figure out lookup limit
					if hasLimit {
						if floatLimit, ok := limit.(float64); ok {
							intLimit = uint64(floatLimit)
						} else {
							return errors.New(fmt.Sprintf("Expecting `limit` number, but you gave me: %v", floatLimit))
						}
					}
					// figure out lookup value
					lookupStrValue := fmt.Sprint(lookupValue)
					// now do lookup!
					if ht, indexScan := src.StrHT[strings.Join(vecPath, ",")]; indexScan {
						// index scan is much prefered
						hashValue := StrHash(lookupValue)
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
							(*result)[docID] = false
						}
					} else {
						// fallback to collection scan
						log.Printf("Query %v is inefficient", q)
						counter := uint64(0)
						src.ForAll(func(id uint64, doc interface{}) bool {
							for _, v := range GetIn(doc, vecPath) {
								if fmt.Sprint(v) == lookupStrValue {
									counter += 1
									(*result)[id] = false
									return counter != intLimit
								}
							}
							return true
						})
					}
				default:
					return errors.New(fmt.Sprintf("Expecting a map of lookup parameters, but you gave me: %v", lookupParam))
				}
			// intersect
			case "n":
				if len(expr) < 3 {
					return errors.New(fmt.Sprintf("Expecting more than two results to intersect, but I only have: %v", expr))
				}
				first := true
				for _, subExpr := range expr[1:] {
					subExprResult := make(map[uint64]bool)
					intersection := make(map[uint64]bool)
					err = EvalQuery(subExpr, src, &subExprResult)
					if err != nil {
						return
					}
					// calculate intersection
					if first {
						*result = subExprResult
					} else {
						for k, _ := range subExprResult {
							if _, inBoth := (*result)[k]; inBoth {
								intersection[k] = false
							}
						}
						*result = intersection
					}
					first = false
				}
			// complement
			case "c":
				if len(expr) < 3 {
					return errors.New(fmt.Sprintf("Expecting more than two results to complement, but I only have: %v", expr))
				}
				for _, subExpr := range expr[1:] {
					subExprResult := make(map[uint64]bool)
					complement := make(map[uint64]bool)
					err = EvalQuery(subExpr, src, &subExprResult)
					if err != nil {
						return
					}
					// calculate complement
					for k, _ := range subExprResult {
						if _, inBoth := (*result)[k]; !inBoth {
							complement[k] = false
						}
					}
					for k, _ := range *result {
						if _, inBoth := subExprResult[k]; !inBoth {
							complement[k] = false
						}
					}
					*result = complement
				}
			// union
			case "u":
				for _, subExpr := range expr[1:] {
					err = EvalQuery(subExpr, src, result)
					if err != nil {
						return
					}
				}
			// all documents
			case "all":
				src.Data.ForAll(func(id uint64, _ []byte) bool {
					(*result)[id] = false
					return true
				})
			default:
				return errors.New(fmt.Sprintf("Unknown query operator '%s'", op))
			}
		default:
			return errors.New(fmt.Sprintf("Unknown query operator '%v'", op))
		}
	}
	return nil
}
